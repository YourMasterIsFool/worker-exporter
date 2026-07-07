use std::sync::Arc;

use anyhow::Result;
use sqlx::{Postgres, Transaction};

use crate::{
    core::{traits::load::BulkLoaderTrait, types::SycnTableResult},
    infrastructure::db::ats_client::AtsClient,
};

use super::common::OltpBulkTableLoader;

pub struct AtsVacancyPicBulkLoader {
    pub client: Arc<AtsClient>,
}

#[async_trait::async_trait]
impl BulkLoaderTrait for AtsVacancyPicBulkLoader {
    #[tracing::instrument(
        skip(self, tx, result),
        fields(types = "sync_bulk_loader", loader = "ATS_VACANCY_PIC")
    )]
    async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        result: &SycnTableResult,
    ) -> Result<i64> {
        OltpBulkTableLoader {
            source_table: "ATS_VACANCY_PIC",
            target_table: "vacancy.vacancy_pic",
            column_map: &[],
            column_castings: &[],
            conflict_columns: &["vacancy_id", "employee_id"],
        }
        .load(tx, result)
        .await
    }
}
