#!/bin/sh
sqlx database reset
cargo build
target/debug/savedb account < data/account_info.csv
target/debug/savedb purchase < data/transactions.csv
