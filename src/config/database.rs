use std::env;

use anyhow::{Ok, Result};
use dotenv::dotenv;
use sqlx::{postgres::PgPoolOptions, PgPool};

pub struct DatabaseConfig {
    olap_database_url: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseConfig {
    pub fn new() -> Self {
        dotenv().ok();

        let olap_database_url = env::var("OLAP_DATABASE_URL").expect("DATABASE OLAP NOT DEFINED");

        Self { olap_database_url }
    }

    pub async fn get_pg_pool(&self) -> Result<PgPool> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(self.olap_database_url.as_str())
            .await
            .expect("Failed running pg pool");

        Ok(pool)
    }
}
