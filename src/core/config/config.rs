use std::env;

use dotenv::dotenv;

pub struct Config {
    pub(crate) warehouse_db_url: String,
    pub(crate) organization_db_url: String,
    // pub(crate) hris_db_url: String,
}

impl Config {
    pub fn new() -> Self {
        dotenv().ok();

        let warehouse_database_url =
            env::var("WAREHOUSE_DATABASE_URL").expect("WAREHOUSE DATABASE URL NOT DEFINED ");
        let organization_database_url =
            env::var("ORGANIZATION_DATABASE_URL").expect("ORGANIZATION DATABASE URL NOT DEFINED ");
        // let warehouse_database_url =
        //     env::var("WAREHOUSE_DATABASE_URL").expect("WAREHOUSE DATABASE URL NOT DEFINED ");
        // let hris_database_url = env::var("HRIS_DATABASE_URL").expect(" HRIS URL NOT DEFINED ");
        Self {
            warehouse_db_url: warehouse_database_url,
            organization_db_url: organization_database_url,
        }
    }
}
