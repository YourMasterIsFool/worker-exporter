use std::collections::HashSet;

use moka::future::Cache;
use serde_json::Value;

use crate::core::{
    traits::etl::DataFetcherJson,
    types::{CastingEnum, WhereField},
};

pub fn normalized_lookup_token(value: &str) -> String {
    value.trim().to_lowercase()
}

pub fn lookup_operator(casting: Option<&CastingEnum>) -> &'static str {
    match casting {
        Some(CastingEnum::Uuid)
        | Some(CastingEnum::Number)
        | Some(CastingEnum::Date)
        | Some(CastingEnum::Enum(_)) => "=",
        Some(CastingEnum::Varchar) | None => "ILIKE",
    }
}

pub fn values_filter(
    field: impl Into<String>,
    operator: impl Into<String>,
    values: Vec<String>,
    casting: Option<CastingEnum>,
) -> WhereField {
    WhereField {
        field: field.into(),
        operator: operator.into(),
        value: None,
        values: Some(values),
        casting,
    }
}

pub fn uuid_values_filter(field: impl Into<String>, values: Vec<String>) -> WhereField {
    values_filter(field, "=", values, Some(CastingEnum::Uuid))
}

pub fn fetcher_cache_key(fetcher_name: &str, value: &str) -> String {
    format!(
        "{}:{}",
        fetcher_name.trim().to_uppercase(),
        normalized_lookup_token(value)
    )
}

pub fn cache_value_token(value: &Value) -> Option<String> {
    match value {
        Value::String(value) if !value.trim().is_empty() => Some(value.trim().to_string()),
        Value::Null => None,
        value => Some(value.to_string()),
    }
}

/// Resolves one lookup value and stores the fetched row under the same cache-key
/// convention used by the import orchestrator.
pub async fn fetch_one_with_optional_cache(
    fetcher: &(dyn DataFetcherJson + Send + Sync),
    fetcher_name: &str,
    value: &str,
    fetcher_field_key: &str,
    casting: Option<CastingEnum>,
    use_cache: bool,
    cache: &Cache<String, Value>,
) -> anyhow::Result<Option<Value>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }

    let cache_key = fetcher_cache_key(fetcher_name, value);
    tracing::info!("Mengecek cache key: {}", cache_key);

    if use_cache {
        if let Some(cached_value) = cache.get(&cache_key).await {
            tracing::info!("🔥 CACHE HIT BERHASIL: {}", cache_key);
            return Ok(Some(cached_value));
        }
        tracing::info!("🧊 Cache MISS untuk key: {}", cache_key);
    }

    let rows = fetcher
        .fetch(Some(vec![values_filter(
            fetcher_field_key,
            lookup_operator(casting.as_ref()),
            vec![value.to_string()],
            casting,
        )]))
        .await?;

    let row = rows.into_iter().next();
    if let Some(row) = row.as_ref().filter(|_| use_cache) {
        tracing::info!("💾 Menyimpan ke cache lokal: {}", cache_key);
        cache.insert(cache_key, row.clone()).await;
    }

    Ok(row)
}

/// Resolves a set of lookup values. Cached values are returned immediately and
/// only cache misses are sent to the registered fetcher in a single query.
pub async fn fetch_many_with_optional_cache(
    fetcher: &(dyn DataFetcherJson + Send + Sync),
    fetcher_name: &str,
    requested_values: Vec<String>,
    fetcher_field_key: &str,
    fetcher_key: &str,
    casting: Option<CastingEnum>,
    use_cache: bool,
    cache: Option<&Cache<String, Value>>,
) -> anyhow::Result<Vec<Value>> {
    if requested_values.is_empty() {
        return Ok(Vec::new());
    }

    let Some(cache) = cache.filter(|_| use_cache) else {
        return fetcher
            .fetch(Some(vec![values_filter(
                fetcher_field_key,
                lookup_operator(casting.as_ref()),
                requested_values,
                casting,
            )]))
            .await;
    };

    let mut rows = Vec::new();
    let mut missing = Vec::new();
    let mut seen_missing = HashSet::new();

    for value in requested_values {
        let cache_key = fetcher_cache_key(fetcher_name, &value);
        if let Some(row) = cache.get(&cache_key).await {
            tracing::info!("🔥 CACHE HIT BERHASIL: {}", cache_key);
            rows.push(row);
        } else if seen_missing.insert(value.clone()) {
            missing.push(value);
        }
    }

    if missing.is_empty() {
        return Ok(rows);
    }

    tracing::info!(
        "🔥 BATCH FETCH MISS: Memanggil Fetcher '{}' untuk {} data...",
        fetcher_name,
        missing.len()
    );

    let fetched_rows = fetcher
        .fetch(Some(vec![values_filter(
            fetcher_field_key,
            lookup_operator(casting.as_ref()),
            missing,
            casting,
        )]))
        .await?;

    for row in &fetched_rows {
        let cache_values = [fetcher_key, fetcher_field_key]
            .into_iter()
            .filter_map(|field| row.get(field))
            .filter_map(cache_value_token)
            .collect::<HashSet<_>>();

        for value in cache_values {
            let cache_key = fetcher_cache_key(fetcher_name, &value);
            tracing::info!("💾 Menyimpan ke cache lokal: {}", cache_key);
            cache.insert(cache_key, row.clone()).await;
        }
    }

    rows.extend(fetched_rows);
    Ok(rows)
}
