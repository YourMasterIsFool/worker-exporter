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

pub struct AtsVacancyStageBulkLoader {
    pub client: Arc<AtsClient>,
}

#[async_trait::async_trait]
impl BulkLoaderTrait for AtsVacancyStageBulkLoader {
    #[tracing::instrument(
        skip(self, tx, result),
        fields(types = "sync_bulk_loader", loader = "ATS_VACANCY_STAGE")
    )]
    async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        result: &SycnTableResult,
    ) -> Result<i64> {
        let column_castings = [ColumnCasting {
            column: "stage_email_template_reject",
            casting: CastingEnum::Uuid,
        }];

        OltpBulkTableLoader {
            source_table: "ATS_VACANCY_STAGE",
            target_table: "vacancy.vacancy_stage",
            column_map: &[],
            column_castings: &column_castings,
            conflict_columns: &["id"],
        }
        .load(tx, result)
        .await
    }
}
