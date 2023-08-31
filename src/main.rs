use clap::Parser;
use savedb::*;
use std::error::Error;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv()?;
    let cli = Cli::parse();
    let pool = connect_db().await?;
    let reader = build_reader();

    match cli.command {
        Command::Account => account::parse_and_upload(reader, &pool).await,
        Command::Purchase => purchase::parse_and_upload(reader, &pool).await,
        Command::Rule1 => todo!(),
        Command::Rule2 => todo!(),
    }
}
