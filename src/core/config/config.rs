use std::env;

use dotenv::dotenv;
use tracing::info;

#[derive(Debug)]
pub struct Config {
    pub(crate) warehouse_db_url: String,
    pub(crate) organization_db_url: String,
    pub host: String,
    pub facilitator_grpc_url: String,
    pub hris_db_url: String,
    pub redis_url: String,
    pub ats_grpc_url: String,
    pub port: String,
    pub(crate) master_db_url: String, // pub(crate) hris_db_url: String,
    pub(crate) ats_db_url: String,    // pub(crate) hris_db_url: String,
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl Config {
    pub fn new() -> Self {
        dotenv().ok();

        let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
        let warehouse_database_url =
            env::var("WAREHOUSE_DATABASE_URL").expect("WAREHOUSE DATABASE URL NOT DEFINED ");
        let organization_database_url =
            env::var("ORGANIZATION_DATABASE_URL").expect("ORGANIZATION DATABASE URL NOT DEFINED ");
        let redis_url = env::var("REDIS_URL").expect("REDIS_URL_GAGAL");
        info!("redis url {}", redis_url);
        let master_database_url =
            env::var("MASTER_DATABASE_URL").expect("MASTER DATABASE URL NOT DEFINED ");
        let ats_database_url = env::var("ATS_DATABASE_URL").expect("ATS DATABASE URL NOT DEFINED ");
        let hris_database_url =
            env::var("HRIS_DATABASE_URL").expect("HRIS DATABASE URL NOT DEFINED ");
        let facilitator_grpc_url =
            env::var("FACILITATOR_GRPC_URL").expect("FACILITATOR GRPC URL NOT DEFINED ");

        let ats_grpc_url = env::var("ATS_GRPC_URL").expect("ATS GRPC URL NOT DEFINED");
        Self {
            warehouse_db_url: warehouse_database_url,
            organization_db_url: organization_database_url,
            master_db_url: master_database_url,
            ats_db_url: ats_database_url,
            hris_db_url: hris_database_url,
            host,
            port,
            facilitator_grpc_url,
            ats_grpc_url,
            redis_url,
        }
    }
}
