use sqlx::PgPool;

pub struct OrganizatinPostgresClient {
    pub pool: PgPool,
}

impl OrganizatinPostgresClient {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
