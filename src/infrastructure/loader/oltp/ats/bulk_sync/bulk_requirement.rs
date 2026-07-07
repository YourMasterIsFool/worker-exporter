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

pub struct AtsVacancyRequirementBulkLoader {
    pub client: Arc<AtsClient>,
}

#[async_trait::async_trait]
impl BulkLoaderTrait for AtsVacancyRequirementBulkLoader {
    #[tracing::instrument(
        skip(self, tx, result),
        fields(types = "sync_bulk_loader", loader = "ATS_VACANCY_REQUIREMENT")
    )]
    async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        result: &SycnTableResult,
    ) -> Result<i64> {
        let column_castings = [
            ColumnCasting {
                column: "field_type",
                casting: CastingEnum::Enum(
                    "vacancy.vacancy_requirement_field_type_enum".to_string(),
                ),
            },
            ColumnCasting {
                column: "field_purpose",
                casting: CastingEnum::Enum(
                    "vacancy.vacancy_requirement_field_purpose_enum".to_string(),
                ),
            },
            ColumnCasting {
                column: "compare_operator",
                casting: CastingEnum::Enum(
                    "vacancy.vacancy_requirement_compare_operator_enum".to_string(),
                ),
            },
        ];

        OltpBulkTableLoader {
            source_table: "ATS_VACANCY_REQUIREMENT",
            target_table: "vacancy.vacancy_requirement",
            column_map: &[],
            column_castings: &column_castings,
            conflict_columns: &["id"],
        }
        .load(tx, result)
        .await
    }
}
