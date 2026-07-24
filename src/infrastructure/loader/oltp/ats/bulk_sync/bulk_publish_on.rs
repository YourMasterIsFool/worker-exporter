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

/// Bulk loader untuk relasi publikasi vacancy ke platform careersite.
pub struct AtsVacancyPublishOnBulkLoader {
    pub client: Arc<AtsClient>,
}

#[async_trait::async_trait]
impl BulkLoaderTrait for AtsVacancyPublishOnBulkLoader {
    #[tracing::instrument(
        skip(self, tx, result),
        fields(types = "sync_bulk_loader", loader = "ATS_VACANCY_PUBLISH_ON")
    )]
    async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        result: &SycnTableResult,
    ) -> Result<i64> {
        let column_castings = [
            ColumnCasting {
                column: "platform",
                casting: CastingEnum::Enum("vacancy.vacancy_publish_on_platform_enum".to_string()),
            },
            ColumnCasting {
                column: "status",
                casting: CastingEnum::Enum("vacancy.vacancy_publish_on_status_enum".to_string()),
            },
        ];

        OltpBulkTableLoader {
            source_table: "ATS_VACANCY_PUBLISH_ON",
            target_table: "vacancy.vacancy_publish_on",
            column_map: &[],
            column_castings: &column_castings,
            conflict_columns: &["id"],
        }
        .load(tx, result)
        .await
    }
}
