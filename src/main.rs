use anyhow::Result;

use crate::boostrap::bootstrap;

mod app_state;
mod application;
mod boostrap;
mod config;
mod core;
mod domain;
mod errors;
mod infrastructure;
mod repo;
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    if let Err(e) = bootstrap().await {
        eprintln!("fatal error: {:?}", e);
        std::process::exit(1);
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    Ok(())
}
