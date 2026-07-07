use std::sync::Arc;

use anyhow::Result;
use sqlx::{Postgres, Transaction};

use crate::{
    core::{
        traits::load::BulkLoaderTrait,
        types::{CastingEnum, SycnTableResult},
    },
    infrastructure::db::ats_client::AtsClient,
};

use super::common::{ColumnCasting, OltpBulkTableLoader};

pub struct AtsVacancyBulkLoader {
    pub client: Arc<AtsClient>,
}

#[async_trait::async_trait]
impl BulkLoaderTrait for AtsVacancyBulkLoader {
    #[tracing::instrument(
        skip(self, tx, result),
        fields(types = "sync_bulk_loader", loader = "ATS_VACANCY")
    )]
    async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        result: &SycnTableResult,
    ) -> Result<i64> {
        let column_castings = [ColumnCasting {
            column: "status",
            casting: CastingEnum::Enum("vacancy.vacancy_status_enum".to_string()),
        }];

        OltpBulkTableLoader {
            source_table: "ATS_VACANCY",
            target_table: "vacancy.vacancy",
            column_map: &[("current_hired", "currentHired")],
            column_castings: &column_castings,
            conflict_columns: &["id"],
        }
        .load(tx, result)
        .await
    }
}
