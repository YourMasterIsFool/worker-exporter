use sqlx::PgPool;

pub struct HrisClient {
    pub pool: PgPool,
}

impl HrisClient {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
