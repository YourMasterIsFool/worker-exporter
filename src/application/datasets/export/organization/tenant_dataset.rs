use std::sync::Arc;

use sqlx::QueryBuilder;

use crate::core::{
    traits::{
        dataset_trait::Dataset, extractor::incremental_source::IncrementalSource, load::LoadEnum,
    },
    types::{ExportRow, WhereField},
};
use crate::infrastructure::db::warehouse_client::WarehouseClient;
use crate::infrastructure::loader::warehouse::snapshots::organization::tenants::query::TenantPoolQuerySnapshot;

#[derive(Clone)]
pub struct ExportTenantDataset {
    pub client: Arc<WarehouseClient>,
}

#[async_trait::async_trait]
impl Dataset for ExportTenantDataset {
    async fn get_min_cursor(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<String> {
        let sql = format!(" SELECT id from {} ", "tenant_snapshots");

        let mut qb = QueryBuilder::new(&sql);

        if let Some(filters) = filters {
            qb.push(" WHERE 1=1 ");
            for field in filters {
                qb.push(format!(" AND {} {} ", field.field, field.operator))
                    .push_bind(field.value);
            }
        }

        qb.push(" ORDER BY id ASC limit 1");

        let result = qb
            .build_query_scalar::<String>()
            .fetch_one(&self.client.pool)
            .await?;

        Ok(result)
    }
    async fn fetch_batch(
        &self,
        strategy_enum: &LoadEnum,
        cursor: Option<String>,
        limit: i64,
    ) -> anyhow::Result<(Vec<ExportRow>, Option<String>)> {
        let mut columns = vec![];
        let _filters: Option<Vec<WhereField>> = None;
        if let LoadEnum::Select(_f, c) = strategy_enum {
            if let Some(c) = c {
                columns = c.clone();
            }
        }

        let tenant_snapshot = TenantPoolQuerySnapshot {
            pool: self.client.clone(),
        };
        let tenants = tenant_snapshot.fetch(strategy_enum, limit, cursor).await?;

        let next_cursor = tenants
            .last()
            .and_then(|f| f.get("id"))
            .map(|f| f.to_string());

        let rows: Vec<ExportRow> = tenants
            .into_iter()
            .map(|t| ExportRow::validate_json(t, &columns))
            .collect::<anyhow::Result<Vec<ExportRow>>>()?;
        Ok((rows, next_cursor))
    }
}
