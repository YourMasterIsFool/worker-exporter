use sqlx::MySqlPool;

pub struct MySqlClient {
    pool: MySqlPool,
}

impl MySqlClient {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
}
