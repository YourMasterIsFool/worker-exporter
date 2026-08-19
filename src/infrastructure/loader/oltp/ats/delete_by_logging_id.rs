use sqlx::{Postgres, Transaction};

use crate::core::traits::load::RollbackLoader;

/// A fixed-table rollback loader. `table` is registered in code, never taken
/// from the message payload, so the DELETE target cannot be SQL-injected.
pub struct DeleteByLoggingId {
    pub table: &'static str,
}

#[async_trait::async_trait]
impl RollbackLoader for DeleteByLoggingId {
    async fn rollback(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        logging_id: &str,
    ) -> anyhow::Result<i64> {
        let query = format!("DELETE FROM {} WHERE logging_id::text = $1", self.table);
        let result = sqlx::query(&query)
            .bind(logging_id)
            .execute(&mut **tx)
            .await?;
        Ok(result.rows_affected() as i64)
    }
}
