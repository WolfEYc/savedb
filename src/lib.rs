use chrono::{NaiveDate, TimeZone, Utc};
use clap::{Parser, Subcommand};
use csv::{Reader, ReaderBuilder, Trim};
use serde::{Deserialize, Deserializer};
use sqlx::{MySql, MySqlPool, Pool};
use std::{
    env,
    error::Error,
    io::{self, Stdin},
};

const BIND_LIMIT: usize = u16::MAX as usize;
const MYSQL_DATE_FORMAT: &'static str = "%Y-%m-%d";
const MYSQL_DATETIME_FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

pub mod account;
pub mod purchase;

#[derive(Parser)]
#[command(author="Isaac Wolf", version="0.1.0", about="cli for csv parser and uploader", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Parses and uploads all accounts to the db
    Account,
    /// Parses and uploads all purchases and merchants to the db
    Purchase
}

pub fn build_reader() -> Reader<Stdin> {
    ReaderBuilder::new()
        .trim(Trim::All)
        .from_reader(io::stdin())
}

pub async fn connect_db() -> Result<Pool<MySql>, Box<dyn Error>> {
    let connection_str = &env::var("DATABASE_URL")?;
    Ok(MySqlPool::connect(connection_str).await?)
}

pub fn deserialize_date<'de, D>(deserializer: D, format: &'static str) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    NaiveDate::parse_from_str(&s, format)
        .map_err(serde::de::Error::custom)
        .map(|date| date.format(MYSQL_DATE_FORMAT).to_string())
}

pub fn deserialize_datetime<'de, D>(
    deserializer: D,
    format: &'static str,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Utc.datetime_from_str(&s, format)
        .map_err(serde::de::Error::custom)
        .map(|datetime| datetime.format(MYSQL_DATETIME_FORMAT).to_string())
}
