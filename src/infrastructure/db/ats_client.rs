use sqlx::PgPool;

pub struct AtsClient {
    pub pool: PgPool,
}

impl AtsClient {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
