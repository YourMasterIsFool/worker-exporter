use sqlx::PgPool;

pub struct MasterClient {
    pub pool: PgPool,
}

impl MasterClient {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
