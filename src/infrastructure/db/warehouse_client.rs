use anyhow::Ok;
use sqlx::PgPool;
pub struct WarehouseClient {
    pub pool: PgPool,
}

impl WarehouseClient {
    pub fn new(pool: PgPool) -> anyhow::Result<Self> {
        Ok(Self { pool })
    }
}
