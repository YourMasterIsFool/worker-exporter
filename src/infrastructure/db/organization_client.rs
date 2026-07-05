use sqlx::PgPool;

pub struct OrganizationClient {
    pub pool: PgPool,
}

impl OrganizationClient {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
