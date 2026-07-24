use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use futures_util::future::try_join_all;
use moka::future::Cache;
use rust_xlsxwriter::{
    DataValidation, DataValidationRule, Format, FormatAlign, FormatBorder, Formula, Workbook,
    Worksheet,
};
use serde::Deserialize;
use serde_json::Value;

use crate::{application::fetch_source_regitry::DataFetcherJsonRegistry, core::types::WhereField};

pub struct GeneratedEmailTemplateUsecase {
    pub fetcher: Arc<DataFetcherJsonRegistry>,
    lookup_cache: Cache<String, Arc<[LookupRow]>>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ValueType {
    MultipleSelect,
    Select,
    Number,
    Text,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExampleValue {
    pub value: Option<Value>,
    #[serde(alias = "valueType")]
    pub value_type: Option<ValueType>,
    pub fetcher: Option<String>,
    /// Pilihan statis untuk field select/multipleSelect. Jika berisi, pilihan
    /// ini dipakai dan fetcher tidak dipanggil.
    #[serde(default)]
    pub options: Option<Vec<String>>,
    pub filter: Option<Vec<WhereField>>,
    #[serde(default, alias = "usingCache")]
    pub using_cache: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TemplateHeader {
    pub header: String,
    #[serde(default, alias = "backgroundColor")]
    pub background_color: Option<String>,
}

/// Representasi satu baris lookup table (id + name)
#[derive(Clone)]
struct LookupRow {
    id: String,
    name: String,
}

impl GeneratedEmailTemplateUsecase {
    pub fn new(fetcher: Arc<DataFetcherJsonRegistry>) -> Self {
        Self {
            fetcher,
            lookup_cache: Cache::builder()
                .time_to_live(Duration::from_secs(5 * 60))
                .max_capacity(100)
                .build(),
        }
    }

    fn lookup_cache_key(
        fetcher_name: &str,
        filters: &Option<Vec<WhereField>>,
        display_field: &str,
    ) -> String {
        let mut filters = filters.clone().unwrap_or_default();
        for filter in &mut filters {
            if let Some(values) = &mut filter.values {
                values.sort();
            }
        }
        filters.sort_by(|left, right| {
            serde_json::to_string(left)
                .unwrap_or_default()
                .cmp(&serde_json::to_string(right).unwrap_or_default())
        });

        format!(
            "generated-import-template:{}:{}:{}",
            fetcher_name.trim().to_uppercase(),
            display_field,
            serde_json::to_string(&filters).unwrap_or_default()
        )
    }

    async fn fetch_lookup_rows(
        &self,
        fetcher_name: &str,
        filters: Option<Vec<WhereField>>,
        display_field: &str,
        using_cache: bool,
    ) -> anyhow::Result<Arc<[LookupRow]>> {
        if !using_cache {
            tracing::debug!(
                fetcher = fetcher_name,
                using_cache,
                "Template lookup cache dilewati"
            );
            let rows = self.fetcher.get(fetcher_name)?.fetch(filters).await?;
            return Ok(lookup_rows(rows, fetcher_name, display_field));
        }

        let cache_key = Self::lookup_cache_key(fetcher_name, &filters, display_field);
        if let Some(rows) = self.lookup_cache.get(&cache_key).await {
            tracing::info!(fetcher = fetcher_name, "Template lookup cache hit");
            return Ok(rows);
        }

        tracing::info!(fetcher = fetcher_name, "Template lookup cache miss");
        let fetcher = self.fetcher.get(fetcher_name)?;
        let fetcher_name = fetcher_name.to_owned();
        let display_field = display_field.to_owned();
        self.lookup_cache
            .try_get_with(cache_key, async move {
                let rows = fetcher.fetch(filters).await?;
                Ok::<_, anyhow::Error>(lookup_rows(rows, &fetcher_name, &display_field))
            })
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    pub async fn execute(
        &self,
        headers: &[TemplateHeader],
        example_values: &[ExampleValue],
        payload: serde_json::Value,
    ) -> anyhow::Result<Vec<u8>> {
        log_template_memory(
            "before_workbook",
            headers.len(),
            example_values.len(),
            0,
            0,
            0,
        );
        let mut workbook = Workbook::new();

        // check validation tenant_id must be exists
        if payload.get("tenant_id").is_none() {
            return Err(anyhow::anyhow!(
                "tenant_id not found; please include tenant_id in payload"
            ));
        }

        payload
            .get("tenant_id")
            .and_then(|value| value.as_str())
            .context("tenant_id is required and must be a string")?;

        // ===== Sheet 1: main_import =====
        {
            let main_sheet = workbook.add_worksheet().set_name("main_import")?;
            let example_format = Format::new().set_border(FormatBorder::Thin);

            for (col, header) in headers.iter().enumerate() {
                let column = col as u16;
                let mut header_format = Format::new()
                    .set_bold()
                    .set_border(FormatBorder::Thin)
                    .set_align(FormatAlign::Center);

                if let Some(background_color) = header.background_color.as_deref() {
                    header_format = header_format.set_background_color(background_color);
                }

                main_sheet.write_string_with_format(0, column, &header.header, &header_format)?;
                main_sheet.set_column_width(
                    column,
                    (header.header.chars().count() as f64 + 2.0).clamp(14.0, 40.0),
                )?;

                if let Some(example) = example_values.get(col) {
                    if let Some(value) = example.value.as_ref() {
                        let value = example_value_to_string(value, example.value_type);
                        if matches!(example.value_type, Some(ValueType::Number)) {
                            if let Ok(number) = value.parse::<f64>() {
                                main_sheet.write_number_with_format(
                                    1,
                                    column,
                                    number,
                                    &example_format,
                                )?;
                            } else {
                                main_sheet.write_string_with_format(
                                    1,
                                    column,
                                    &value,
                                    &example_format,
                                )?;
                            }
                        } else {
                            main_sheet.write_string_with_format(
                                1,
                                column,
                                &value,
                                &example_format,
                            )?;
                        }
                    }
                }
            }
        }

        // Master lookup sepenuhnya berasal dari `example_value` payload.
        // Posisi entry wajib sama dengan `headers`, sehingga filter tidak dapat
        // tertukar oleh konfigurasi master yang di-hard-code.
        let lookup_fetches = try_join_all(example_values.iter().enumerate().filter_map(
            |(header_index, example)| {
                if !matches!(
                    example.value_type,
                    Some(ValueType::Select | ValueType::MultipleSelect)
                ) {
                    return None;
                }

                let options = example.options.clone().unwrap_or_default();
                let fetcher_name = example
                    .fetcher
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned);
                let filters = example.filter.clone();
                let using_cache = example.using_cache;
                let display_field = fetcher_name
                    .as_deref()
                    .map(lookup_display_field)
                    .unwrap_or("name");
                let title = headers
                    .get(header_index)
                    .map(|header| header.header.clone())
                    .unwrap_or_else(|| fetcher_name.clone().unwrap_or_else(|| "Options".into()));

                Some(async move {
                    let (rows, lookup_key, fetcher_name) = if !options.is_empty() {
                        let rows: Arc<[LookupRow]> = options
                            .iter()
                            .map(|value| LookupRow {
                                id: value.clone(),
                                name: value.clone(),
                            })
                            .filter(|row| !row.name.trim().is_empty())
                            .collect::<Vec<_>>()
                            .into();
                        let lookup_key: String = format!(
                            "static-options:{}",
                            serde_json::to_string(
                                &rows
                                    .iter()
                                    .map(|row: &LookupRow| &row.name)
                                    .collect::<Vec<_>>()
                            )?
                        );
                        (rows, lookup_key, "__OPTIONS__".to_owned())
                    } else {
                        let fetcher_name = fetcher_name.ok_or_else(|| {
                            anyhow::anyhow!("fetcher is required when options is empty")
                        })?;
                        let rows = self
                            .fetch_lookup_rows(
                                &fetcher_name,
                                filters.clone(),
                                display_field,
                                using_cache,
                            )
                            .await?;
                        let lookup_key =
                            Self::lookup_cache_key(&fetcher_name, &filters, display_field);
                        (rows, lookup_key, fetcher_name)
                    };
                    Ok::<_, anyhow::Error>((
                        header_index,
                        title,
                        fetcher_name,
                        filters,
                        display_field,
                        rows,
                        lookup_key,
                    ))
                })
            },
        ));

        let lookups = lookup_fetches.await?;
        let lookup_row_count = lookups
            .iter()
            .map(|(_, _, _, _, _, rows, _)| rows.len())
            .sum::<usize>();
        log_template_memory(
            "after_lookup",
            headers.len(),
            example_values.len(),
            lookup_row_count,
            0,
            0,
        );

        let master_sheet = workbook.add_worksheet().set_name("master_template")?;
        let mut lookup_ranges: HashMap<String, String> = HashMap::new();
        let mut named_ranges = Vec::new();
        let mut validation_ranges = Vec::new();
        let mut lookup_table_count = 0usize;

        for (header_index, title, fetcher_name, filters, display_field, rows, lookup_key) in lookups
        {
            if let Some(range_name) = lookup_ranges.get(&lookup_key) {
                validation_ranges.push((header_index, range_name.clone()));
                continue;
            }

            if rows.is_empty() {
                continue;
            }

            let start_col = (lookup_table_count * 3) as u16;
            lookup_table_count += 1;
            write_lookup_table(master_sheet, start_col, &title, &rows)?;
            master_sheet.set_column_width(start_col, 22)?;
            master_sheet.set_column_width(start_col + 1, 28)?;

            let range_name = format!("example_options_{header_index}");
            let display_column = excel_column_name(start_col + 1);
            named_ranges.push((
                range_name.clone(),
                format!(
                    "='master_template'!${display_column}$3:${display_column}${}",
                    rows.len() + 2
                ),
            ));
            lookup_ranges.insert(lookup_key, range_name.clone());
            validation_ranges.push((header_index, range_name));
        }

        drop(master_sheet);
        for (range_name, formula) in named_ranges {
            workbook.define_name(&range_name, &formula)?;
        }

        let main_sheet = workbook.worksheet_from_name("main_import")?;
        for (header_index, range_name) in validation_ranges {
            // Dropdown tetap tersedia sebagai bantuan input, tetapi Excel tidak
            // boleh memblokir nilai manual. Ini membuat input `select` dan
            // `multipleSelect` menerima perbedaan uppercase/lowercase maupun
            // nilai yang diketik langsung oleh pengguna.
            let mut validation =
                DataValidation::new().allow_list_formula(Formula::new(format!("={range_name}")));
            validation = validation.show_error_message(false);

            main_sheet.add_data_validation(
                // Row 0 is the header. Row 1 is the first import row (the
                // importer skips only the header), so validation must start
                // there as well.
                1,
                header_index as u16,
                1000,
                header_index as u16,
                &validation,
            )?;
        }

        // Field bertipe number hanya menerima angka di Excel. Excel akan
        // menampilkan error dan menolak nilai teks (style Stop adalah default).
        for (header_index, example) in example_values.iter().enumerate() {
            if !matches!(example.value_type, Some(ValueType::Number)) {
                continue;
            }

            let validation = DataValidation::new()
                .allow_decimal_number(DataValidationRule::Between(-1.0e308_f64, 1.0e308_f64))
                .set_error_title("Invalid number")?
                .set_error_message("Please enter a numeric value.")?;
            main_sheet.add_data_validation(
                1,
                header_index as u16,
                1000,
                header_index as u16,
                &validation,
            )?;
        }

        log_template_memory(
            "before_serialize",
            headers.len(),
            example_values.len(),
            lookup_row_count,
            0,
            0,
        );
        let xlsx = workbook.save_to_buffer()?;
        log_template_memory(
            "after_serialize",
            headers.len(),
            example_values.len(),
            lookup_row_count,
            0,
            xlsx.len(),
        );
        Ok(xlsx)
    }
}

fn log_template_memory(
    stage: &'static str,
    header_count: usize,
    example_count: usize,
    master_lookup_rows: usize,
    dynamic_lookup_rows: usize,
    xlsx_bytes: usize,
) {
    let rss_bytes = process_rss_bytes();
    tracing::info!(
        stage,
        header_count,
        example_count,
        master_lookup_rows,
        dynamic_lookup_rows,
        xlsx_bytes,
        rss_bytes = rss_bytes.unwrap_or_default(),
        rss_available = rss_bytes.is_some(),
        "Generated import template memory usage"
    );
}

#[cfg(target_os = "linux")]
fn process_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?;
    kb.checked_mul(1024)
}

#[cfg(not(target_os = "linux"))]
fn process_rss_bytes() -> Option<u64> {
    None
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => String::new(),
        value => value.to_string(),
    }
}

fn example_value_to_string(value: &Value, value_type: Option<ValueType>) -> String {
    match value {
        Value::Array(values) => values
            .iter()
            .map(|value| example_value_to_string(value, value_type))
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join(","),
        Value::String(value) if matches!(value_type, Some(ValueType::MultipleSelect)) => value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join(","),
        value => value_to_string(value),
    }
}

fn lookup_display_value(row: &Value, fetcher_name: &str) -> String {
    let preferred_fields: &[&str] = match fetcher_name.to_uppercase().as_str() {
        "JOB_POSITION" => &["position_name", "name"],
        "EMPLOYEE" => &["email"],
        "STAGE_TEMPLATE" | "REQUIREMENT_STAGE_TEMPLATE" => &["template_name", "name"],
        "ONBOARDING_TEMPLATE" => &["group_name", "name"],
        "FORM_FIELD" => &["name", "key_field", "label"],
        _ => &[
            "name",
            "label",
            "title",
            "full_name",
            "template_name",
            "code",
        ],
    };

    preferred_fields
        .iter()
        .find_map(|field| row.get(*field).map(value_to_string))
        .filter(|value| !value.is_empty())
        .unwrap_or_default()
}

fn lookup_display_field(fetcher_name: &str) -> &'static str {
    match fetcher_name.to_uppercase().as_str() {
        "JOB_POSITION" => "position_name",
        "EMPLOYEE" => "email",
        "STAGE_TEMPLATE" | "REQUIREMENT_STAGE_TEMPLATE" => "template_name",
        "ONBOARDING_TEMPLATE" => "group_name",
        "FORM_FIELD" => "name",
        _ => "name",
    }
}

fn lookup_rows(raw_rows: Vec<Value>, fetcher_name: &str, display_field: &str) -> Arc<[LookupRow]> {
    raw_rows
        .iter()
        .map(|row| LookupRow {
            id: row.get("id").map(value_to_string).unwrap_or_default(),
            name: row
                .get(display_field)
                .map(value_to_string)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| lookup_display_value(row, fetcher_name)),
        })
        .filter(|row| !row.id.is_empty() && !row.name.is_empty())
        .collect::<Vec<_>>()
        .into()
}

fn excel_column_name(mut column: u16) -> String {
    let mut name = String::new();
    loop {
        name.insert(0, (b'A' + (column % 26) as u8) as char);
        if column < 26 {
            break;
        }
        column = column / 26 - 1;
    }
    name
}

/// Menulis satu tabel lookup (title merged 2 kolom, header id/name, lalu data)
/// di posisi kolom `start_col` pada sheet yang diberikan.
fn write_lookup_table(
    sheet: &mut Worksheet,
    start_col: u16,
    title: &str,
    rows: &[LookupRow],
) -> anyhow::Result<()> {
    // Definisi format dipindahkan ke sini
    let title_format = Format::new()
        .set_bold()
        .set_font_size(14)
        .set_align(FormatAlign::Center);
    let subheader_format = Format::new()
        .set_bold()
        .set_border(FormatBorder::Thin)
        .set_background_color("#D9D9D9");
    let cell_format = Format::new().set_border(FormatBorder::Thin);

    let id_col = start_col;
    let name_col = start_col + 1;

    sheet.merge_range(0, id_col, 0, name_col, title, &title_format)?;
    sheet.write_string_with_format(1, id_col, "id", &subheader_format)?;
    sheet.write_string_with_format(
        1,
        name_col,
        &format!("{} Name", to_title_case(title)),
        &subheader_format,
    )?;

    for (i, row) in rows.iter().enumerate() {
        let r = (i + 2) as u32;
        sheet.write_string_with_format(r, id_col, &row.id, &cell_format)?;
        sheet.write_string_with_format(r, name_col, &row.name, &cell_format)?;
    }
    Ok(())
}
fn to_title_case(s: &str) -> String {
    s.split_whitespace()
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                Some(first) => {
                    first.to_uppercase().collect::<String>() + &c.as_str().to_lowercase()
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}
