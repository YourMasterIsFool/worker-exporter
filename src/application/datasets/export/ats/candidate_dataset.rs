use std::sync::Arc;

use serde::Serialize;
use sqlx::{FromRow, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{
    core::{
        traits::{dataset_trait::Dataset, load::LoadEnum},
        types::{ExportRow, WhereField},
    },
    infrastructure::db::ats_client::AtsClient,
};

const CANDIDATE_TABLE: &str = "candidate.candidate";

#[derive(Debug, FromRow, Serialize)]
struct CandidateExportRow {
    id: Uuid,
    name: String,
    email: String,
}

pub struct CandidateDataset {
    pub client: Arc<AtsClient>,
}

impl CandidateDataset {
    fn tenant_ids(strategy: &LoadEnum) -> anyhow::Result<Vec<String>> {
        let filters = match strategy {
            LoadEnum::Select(Some(filters), _) => filters,
            _ => anyhow::bail!("candidate export requires a tenant filter"),
        };

        let mut tenant_ids = Vec::new();
        for filter in filters {
            let field = filter.field.rsplit('.').next().unwrap_or(&filter.field);
            if field != "tenant_id" {
                anyhow::bail!(
                    "candidate export does not support filter '{}'",
                    filter.field
                );
            }
            if filter.operator != "=" {
                anyhow::bail!("candidate export tenant filter must use '='");
            }
            tenant_ids.extend(
                filter
                    .values
                    .clone()
                    .or_else(|| filter.value.clone().map(|value| vec![value]))
                    .unwrap_or_default(),
            );
        }

        if tenant_ids.is_empty() {
            anyhow::bail!("candidate export requires at least one tenant ID");
        }

        Ok(tenant_ids)
    }

    fn columns(strategy: &LoadEnum) -> Vec<String> {
        match strategy {
            LoadEnum::Select(_, Some(columns)) => columns.clone(),
            _ => Vec::new(),
        }
    }

    fn to_export_row(row: CandidateExportRow, columns: &[String]) -> ExportRow {
        let fields = columns
            .iter()
            .map(|column| match column.as_str() {
                "id" | "candidate_id" => row.id.to_string(),
                "name" => row.name.clone(),
                "email" | "email_address" => row.email.clone(),
                _ => "-".to_string(),
            })
            .collect();
        ExportRow { fields }
    }

    fn append_scope(query: &mut QueryBuilder<'_, Postgres>, tenant_ids: Vec<String>) {
        query
            .push(" WHERE deleted_at IS NULL AND tenant_pool_keys && ")
            .push_bind(tenant_ids)
            .push("::text[]");
    }
}

#[async_trait::async_trait]
impl Dataset for CandidateDataset {
    async fn get_min_cursor(&self, _filters: Option<Vec<WhereField>>) -> anyhow::Result<String> {
        // Batch query uses `id > cursor`; UUID nil retains the first candidate row.
        Ok(Uuid::nil().to_string())
    }

    async fn count(&self, strategy_enum: &LoadEnum) -> anyhow::Result<Option<i64>> {
        let tenant_ids = Self::tenant_ids(strategy_enum)?;
        let mut query =
            QueryBuilder::<Postgres>::new(format!("SELECT COUNT(*) FROM {CANDIDATE_TABLE}"));
        Self::append_scope(&mut query, tenant_ids);

        Ok(Some(
            query
                .build_query_scalar::<i64>()
                .fetch_one(&self.client.pool)
                .await?,
        ))
    }

    async fn fetch_batch(
        &self,
        strategy_enum: &LoadEnum,
        cursor: Option<String>,
        limit: i64,
    ) -> anyhow::Result<(Vec<ExportRow>, Option<String>)> {
        let tenant_ids = Self::tenant_ids(strategy_enum)?;
        let cursor = cursor
            .map(|cursor| Uuid::parse_str(&cursor))
            .transpose()
            .map_err(|error| anyhow::anyhow!("candidate export cursor is invalid: {error}"))?;
        let columns = Self::columns(strategy_enum);

        let mut query =
            QueryBuilder::<Postgres>::new(format!("SELECT id, name, email FROM {CANDIDATE_TABLE}"));
        Self::append_scope(&mut query, tenant_ids);
        if let Some(cursor) = cursor {
            query.push(" AND id > ").push_bind(cursor);
        }
        query.push(" ORDER BY id ASC LIMIT ").push_bind(limit);

        let rows = query
            .build_query_as::<CandidateExportRow>()
            .fetch_all(&self.client.pool)
            .await?;
        let next_cursor = rows.last().map(|row| row.id.to_string());
        let rows = rows
            .into_iter()
            .map(|row| Self::to_export_row(row, &columns))
            .collect();

        Ok((rows, next_cursor))
    }
}

#[cfg(test)]
mod tests {
    use super::{CandidateDataset, CandidateExportRow};
    use crate::core::{traits::load::LoadEnum, types::WhereField};
    use uuid::Uuid;

    #[test]
    fn maps_supported_candidate_columns() {
        let row = CandidateExportRow {
            id: Uuid::nil(),
            name: "Jane Doe".into(),
            email: "jane@example.com".into(),
        };
        let export = CandidateDataset::to_export_row(
            row,
            &["candidate_id".into(), "name".into(), "email".into()],
        );

        assert_eq!(
            export.fields,
            vec![
                Uuid::nil().to_string(),
                "Jane Doe".into(),
                "jane@example.com".into(),
            ]
        );
    }

    #[test]
    fn accepts_only_tenant_scope_filters() {
        let strategy = LoadEnum::Select(
            Some(vec![WhereField {
                field: "tenant_id".into(),
                operator: "=".into(),
                value: Some("tenant-a".into()),
                values: None,
                casting: None,
            }]),
            None,
        );

        assert_eq!(
            CandidateDataset::tenant_ids(&strategy).unwrap(),
            vec!["tenant-a".to_string()]
        );
    }
}
