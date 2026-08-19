use std::sync::Arc;

use anyhow::Context;
use chrono::NaiveDate;
use sqlx::Row;

use crate::{
    core::traits::{orchestrator::AggregateConfig, pipeline::pipeline_trait::AggregatePipeline},
    infrastructure::db::ats_client::AtsClient,
};

pub struct CandidateAggregatePipeline {
    pub ats_client: Arc<AtsClient>,
}

pub struct VacancyAggregatePipeline {
    pub ats_client: Arc<AtsClient>,
}

#[async_trait::async_trait]
impl AggregatePipeline for CandidateAggregatePipeline {
    async fn run(&self, config: &AggregateConfig) -> anyhow::Result<i64> {
        run_period_aggregate(&self.ats_client, config, "candidate.candidate", "candidate").await
    }
}

#[async_trait::async_trait]
impl AggregatePipeline for VacancyAggregatePipeline {
    async fn run(&self, config: &AggregateConfig) -> anyhow::Result<i64> {
        run_period_aggregate(&self.ats_client, config, "vacancy.vacancy", "vacancy").await
    }
}

async fn run_period_aggregate(
    ats_client: &AtsClient,
    config: &AggregateConfig,
    source_table: &str,
    entity_type: &str,
) -> anyhow::Result<i64> {
    if config.aggregate_code.trim().is_empty() {
        anyhow::bail!("aggregate_config.aggregate_code wajib diisi");
    }
    if matches!((config.from, config.to), (Some(from), Some(to)) if from >= to) {
        anyhow::bail!("aggregate_config.to harus setelah aggregate_config.from");
    }

    let source_rows = sqlx::query(&format!(
        r#"
        SELECT
            periods.period_type,
            periods.period_start,
            COUNT(*)::bigint AS total
        FROM {source_table} AS source
        CROSS JOIN LATERAL (
            VALUES
                ('DAILY'::varchar, source.created_at::date),
                ('MONTHLY'::varchar, date_trunc('month', source.created_at)::date),
                ('YEARLY'::varchar, date_trunc('year', source.created_at)::date)
        ) AS periods(period_type, period_start)
        WHERE ($1::date IS NULL OR source.created_at >= $1::date)
          AND ($2::date IS NULL OR source.created_at < $2::date)
        GROUP BY periods.period_type, periods.period_start
        ORDER BY periods.period_type, periods.period_start
        "#
    ))
    .bind(config.from)
    .bind(config.to)
    .fetch_all(&ats_client.pool)
    .await
    .with_context(|| format!("gagal menghitung aggregate {entity_type} dari created_at"))?;

    let scope_key = config.scope_key.as_deref().unwrap_or("GLOBAL");
    if scope_key != "GLOBAL" {
        anyhow::bail!(
            "aggregate {entity_type} sementara hanya mendukung scope_key=GLOBAL karena filter tenant belum diaktifkan"
        );
    }
    let mut transaction = ats_client.pool.begin().await?;
    for row in &source_rows {
        let period_type: String = row.try_get("period_type")?;
        let period_start: NaiveDate = row.try_get("period_start")?;
        let total: i64 = row.try_get("total")?;

        sqlx::query(
            r#"
            INSERT INTO aggregate_period (
                scope_key,
                aggregate_code,
                entity_type,
                period_type,
                period_start,
                dimensions,
                metrics
            )
            VALUES ($1, $2, $3, $4, $5, '{}'::jsonb,
                    jsonb_build_object('total', $6::bigint))
            ON CONFLICT (
                scope_key,
                aggregate_code,
                entity_type,
                period_type,
                period_start,
                dimensions
            ) DO UPDATE SET
                metrics = EXCLUDED.metrics,
                updated_at = now()
            "#,
        )
        .bind(scope_key)
        .bind(&config.aggregate_code)
        .bind(entity_type)
        .bind(period_type)
        .bind(period_start)
        .bind(total)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;

    Ok(source_rows.len() as i64)
}
