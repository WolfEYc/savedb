use chrono::{NaiveDate, Utc, TimeZone};
use serde::{Deserializer, Deserialize};

const BIND_LIMIT: usize = u16::MAX as usize;
const MYSQL_DATE_FORMAT: &'static str = "%Y-%m-%d";
const MYSQL_DATETIME_FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

pub mod account;
pub mod purchase;

pub fn deserialize_date<'de, D>(deserializer: D, format: &'static str) -> Result<String, D::Error> where D: Deserializer<'de> {
    let s = String::deserialize(deserializer)?;
    NaiveDate::parse_from_str(&s, format)
        .map_err(serde::de::Error::custom)
        .map(|date|date.format(MYSQL_DATE_FORMAT).to_string())
}

pub fn deserialize_datetime<'de, D>(deserializer: D, format: &'static str) -> Result<String, D::Error> where D: Deserializer<'de> {
    let s = String::deserialize(deserializer)?;
    Utc.datetime_from_str(&s, format)
        .map_err(serde::de::Error::custom)
        .map(|datetime| datetime.format(MYSQL_DATETIME_FORMAT).to_string())
}