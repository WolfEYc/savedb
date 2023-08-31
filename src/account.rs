use crate::deserialize_date;
use csv::Reader;
use futures::future::join_all;
use serde::{Deserialize, Deserializer};
use sqlx::{mysql::MySqlQueryResult, MySql, Pool, QueryBuilder};
use std::{error::Error, io::Stdin, usize};

const ACCOUNT_ARGS: usize = 12;
const ACCOUNT_CHUNK: usize = crate::BIND_LIMIT / ACCOUNT_ARGS;

const DOB_FORMAT: &'static str = "%m/%d/%Y";

fn deserialize_dob<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_date(deserializer, DOB_FORMAT)
}

fn deserialize_ssn<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .replace("-", "")
        .parse()
        .map_err(serde::de::Error::custom)
}

fn deserialize_unit<'de, D>(deserializer: D) -> Result<Option<i16>, D::Error>
where
    D: Deserializer<'de>,
{
    let str = String::deserialize(deserializer)?;

    if str.is_empty() {
        return Ok(None);
    };

    let parsed: i16 = str
        .replace("#", "")
        .parse()
        .map_err(serde::de::Error::custom)?;

    Ok(Some(parsed))
}

#[derive(Debug, Deserialize, Clone)]
pub struct Account {
    pub last_name: String,
    pub first_name: String,
    pub street_address: String,
    #[serde(deserialize_with = "deserialize_unit")]
    pub unit: Option<i16>,
    pub city: String,
    pub state: String,
    pub zip: i32,
    #[serde(deserialize_with = "deserialize_dob")]
    pub dob: String,
    #[serde(deserialize_with = "deserialize_ssn")]
    pub ssn: i32,
    pub email_address: String,
    pub mobile_number: i64,
    pub account_number: i32,
}

async fn upload_chunk(
    accounts: &[Account],
    pool: &Pool<MySql>,
) -> Result<MySqlQueryResult, sqlx::Error> {
    let mut query_builder: QueryBuilder<MySql> = QueryBuilder::new(
        "REPLACE INTO account(
            account_number,
            mobile_number,
            email_address,
            ssn,
            dob,
            zip,
            account_state,
            city,
            unit,
            street_address,
            first_name,
            last_name
        ) ",
    );

    query_builder.push_values(accounts, |mut b, a: &Account| {
        let a = a.clone();
        b.push_bind(a.account_number)
            .push_bind(a.mobile_number)
            .push_bind(a.email_address)
            .push_bind(a.ssn)
            .push_bind(a.dob)
            .push_bind(a.zip)
            .push_bind(a.state)
            .push_bind(a.city)
            .push_bind(a.unit)
            .push_bind(a.street_address)
            .push_bind(a.first_name)
            .push_bind(a.last_name);
    });

    query_builder.build().execute(pool).await
}

pub async fn upload(accounts: Vec<Account>, pool: &Pool<MySql>) -> Result<(), sqlx::Error> {
    let uploads = accounts
        .chunks(ACCOUNT_CHUNK)
        .map(|chunk| upload_chunk(chunk, pool));

    let upload_results = join_all(uploads).await;

    let result = upload_results.into_iter().find(|r| r.is_err());

    if let Some(Err(err)) = result {
        Err(err)
    } else {
        Ok(())
    }
}

pub fn parse(mut reader: Reader<Stdin>) -> Result<Vec<Account>, Box<dyn Error>> {
    reader
        .deserialize()
        .map(|r| {
            let account: Account = r?;
            Ok(account)
        })
        .collect()
}

pub async fn parse_and_upload(
    reader: Reader<Stdin>,
    pool: &Pool<MySql>,
) -> Result<(), Box<dyn Error>> {
    let accounts = parse(reader)?;
    Ok(upload(accounts, pool).await?)
}
