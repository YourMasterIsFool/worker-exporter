use sqlx::PgPool;

pub struct HrisPostgresClient {
    pool: PgPool,
}

impl HrisPostgresClient {
    pub fn new(pool: PgPool) -> Self {
        Self { pool: pool }
    }
}
