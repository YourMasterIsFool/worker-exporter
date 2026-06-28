use anyhow::{Ok, Result};
use sqlx::PgPool;

use crate::core::config::config::Config;

pub struct DatabaseManager {
    pub warehouse: PgPool,
    // pub hris: PgPool,
    pub organization: PgPool,
    pub ats: PgPool,
    pub master: PgPool,
}

impl DatabaseManager {
    pub async fn new(config: &Config) -> Result<Self> {
        let warehouse_pool = PgPool::connect(&config.warehouse_db_url).await?;
        // let hris_pool = PgPool::connect(&config.hris_db_url).await?;
        let organization_pool = PgPool::connect(&config.organization_db_url).await?;
        let ats_pool = PgPool::connect(&config.ats_db_url).await?;
        let master_pool = PgPool::connect(&config.master_db_url).await?;

        Ok(Self {
            warehouse: warehouse_pool,
            // hris: hris_pool,
            ats: ats_pool,
            organization: organization_pool,
            master: master_pool,
        })
    }
}
