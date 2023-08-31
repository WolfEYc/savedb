use std::{error::Error, io::{Stdin, self}, env};
use csv::{Reader, ReaderBuilder, Trim};
use savedb::*;
use sqlx::MySqlPool;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author="Isaac Wolf", version="0.1.0", about="cli for csv parser and uploader", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command
}

#[derive(Subcommand)]
enum Command {
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv()?;
    let cli = Cli::parse();
    let pool = MySqlPool::connect(&env::var("DATABASE_URL")?).await?;

    let reader = build_reader();

    match cli.command {
        Command::Account => account::parse_and_upload(reader, &pool).await,
        Command::Purchase => purchase::parse_and_upload(reader, &pool).await
    }
}
