use std::{io::Stdin, error::Error, usize};
use csv::Reader;
use futures::future::join_all;
use serde::{Deserialize, Deserializer};
use sqlx::{Pool, MySql, QueryBuilder, mysql::MySqlQueryResult};

use crate::{BIND_LIMIT, deserialize_datetime, deserialize_date};

const PURCHASE_ARGS: usize = 9;
const PURCHASE_CHUNK: usize = BIND_LIMIT / PURCHASE_ARGS;

const PURCHASE_DATETIME_FORMAT:&'static str = "mmddYYY %H:%M:%S";
const POST_DATE_FORMAT: &'static str = "mddYYYY";

fn deserialize_purchase_datetime<'de, D>(deserializer: D) -> Result<String, D::Error> where D: Deserializer<'de> {
    deserialize_datetime(deserializer, PURCHASE_DATETIME_FORMAT)
}

fn deserialize_post_date<'de, D>(deserializer: D) -> Result<String, D::Error> where D: Deserializer<'de> {
    deserialize_date(deserializer, POST_DATE_FORMAT)
}

fn deserialize_transaction_amount<'de, D>(deserializer: D) -> Result<f32, D::Error> where D: Deserializer<'de> {    
    let mut s = String::deserialize(deserializer)?;
    
    match s.pop() {
        None => Err(serde::de::Error::custom("Empty transaction amt")),
        Some('-') => s.parse::<f32>().map(|amt| -amt).map_err(serde::de::Error::custom),
        _ => Err(serde::de::Error::custom("Invalid transaction amt"))
    }
}

fn deserialize_merchant_description<'de, D>(deserializer: D) -> Result<MerchantDescription, D::Error> where D: Deserializer<'de> {
    let s = String::deserialize(deserializer)?;
    
    let state = s.split_whitespace()
        .last().ok_or(serde::de::Error::custom("Merchant state not found"))?
        .get(..2).ok_or(serde::de::Error::custom("Merchant state not found"))?;

    let name = s.rsplit_once(" ")
        .ok_or(serde::de::Error::custom("Merchant name parse error"))?
        .0;

    Ok(MerchantDescription {
        merchant_name: name.to_string(),
        merchant_state: state.to_string()
    })
}

fn is_word(s: &String) -> bool {
    s.chars().all(|c| c.is_alphabetic())
}

fn fix_legagy_merchant_name(purchase: &mut Purchase) {
    if is_word(&purchase.merchant_number) {
        purchase.merchant_description.merchant_name = purchase.merchant_number.clone();
    };
}

#[derive(Debug, Deserialize, Clone)]
pub struct MerchantDescription { 
    pub merchant_name: String,
    pub merchant_state: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Purchase {
    pub account_number: i32,
    #[serde(deserialize_with="deserialize_purchase_datetime")]
    pub transaction_datetime: String,
    #[serde(deserialize_with="deserialize_transaction_amount")]
    pub transaction_amount: f32,
    #[serde(deserialize_with="deserialize_post_date")]
    pub post_date: String,
    pub transaction_number: i32,
    pub merchant_number: String,
    #[serde(deserialize_with="deserialize_merchant_description")]
    pub merchant_description: MerchantDescription,
    pub merchant_category_code: i16
}

async fn upload_chunk(purchases: &[Purchase], pool: &Pool<MySql>) -> Result<MySqlQueryResult, sqlx::Error> {
    let mut query_builder: QueryBuilder<MySql> = QueryBuilder::new(
        "INSERT INTO purchase (
            account_number,
            purchase_datetime,
            purchase_amount,
            post_date,
            purchase_number,
            merchant_number,
            merchant_name,
            merchant_state,
            merchant_category_code)
        ");

    query_builder.push_values(purchases, |mut b, p: &Purchase| {
        let p = p.clone();
        b.push_bind(p.account_number)
        .push_bind(p.transaction_datetime)
        .push_bind(p.transaction_amount)
        .push_bind(p.post_date)
        .push_bind(p.transaction_number)
        .push_bind(p.merchant_number)
        .push_bind(p.merchant_description.merchant_name)
        .push_bind(p.merchant_description.merchant_state)
        .push_bind(p.merchant_category_code);
    });

    query_builder
        .build()
        .execute(pool)
        .await
}

pub async fn upload(purchases: Vec<Purchase>, pool: &Pool<MySql>) -> Result<(), sqlx::Error> {
    let uploads = purchases.chunks(PURCHASE_CHUNK)
    .map(|chunk| upload_chunk(chunk, pool));

    let upload_results = join_all(uploads).await;

    let result = upload_results
    .into_iter()
    .find(|r| r.is_err());

    if let Some(Err(err)) = result {
        Err(err)
    } else {
        Ok(())
    }
}

pub fn parse(mut reader: Reader<Stdin>) -> Result<Vec<Purchase>, Box<dyn Error>> {            
    reader.deserialize()
    .map(|r| -> Result<Purchase, Box<dyn Error>> {
        let mut account: Purchase = r?;
        fix_legagy_merchant_name(&mut account);
        Ok(account)
    }).collect()
}

pub async fn parse_and_upload(reader: Reader<Stdin>, pool: &Pool<MySql>) -> Result<(), Box<dyn Error>> {
    let purchases = parse(reader)?;
    Ok(upload(purchases, pool).await?)
}
