use std::{path::Path, sync::Arc, vec};

use futures_util::future::try_join_all;
use rust_xlsxwriter::{
    DataValidation, Format, FormatAlign, FormatBorder, Formula, Workbook, Worksheet,
};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    application::fetch_source_regitry::DataFetcherJsonRegistry,
    core::types::{CastingEnum, WhereField},
};

pub struct GeneratedEmailTemplateUsecase {
    pub fetcher: Arc<DataFetcherJsonRegistry>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ValueType {
    MultipleSelect,
    Select,
    Text,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExampleValue {
    pub value: Option<Value>,
    #[serde(alias = "valueType")]
    pub value_type: Option<ValueType>,
    pub fetcher: Option<String>,
    pub filter: Option<Vec<WhereField>>,
}

/// Representasi satu baris lookup table (id + name)
struct LookupRow {
    id: String,
    name: String,
}

struct MasterConfig {
    title: String,
    fetcher_key: String,
    filter: Option<Vec<WhereField>>,
    // Field opsional jika nama kolom di JSON bukan "name"
    name_field: &'static str,
}
impl GeneratedEmailTemplateUsecase {
    pub async fn execute(
        &self,
        headers: &[String],
        example_values: &[ExampleValue],
        file_path: &str,
        tenant_id: &str,
    ) -> anyhow::Result<()> {
        let path = Path::new(&file_path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let mut workbook = Workbook::new();

        // ===== Sheet 1: main_import =====
        {
            let main_sheet = workbook.add_worksheet().set_name("main_import")?;
            let header_format = Format::new()
                .set_bold()
                .set_border(FormatBorder::Thin)
                .set_align(FormatAlign::Center);
            let example_format = Format::new().set_border(FormatBorder::Thin);

            for (col, header) in headers.iter().enumerate() {
                let column = col as u16;
                main_sheet.write_string_with_format(0, column, header, &header_format)?;
                main_sheet.set_column_width(
                    column,
                    (header.chars().count() as f64 + 2.0).clamp(14.0, 40.0),
                )?;

                if let Some(example) = example_values.get(col) {
                    if let Some(value) = example.value.as_ref() {
                        let value = example_value_to_string(value, example.value_type);
                        main_sheet.write_string_with_format(1, column, &value, &example_format)?;
                    }
                }
            }
        }

        // ===== Fetch data master =====
        let province_filter = vec![WhereField {
            field: "level".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec!["adm1".into()]),
            casting: Some(CastingEnum::Enum("geo_nodes_level_enum".into())),
        }];
        let city_filter = vec![WhereField {
            field: "level".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec!["adm2".into()]),
            casting: Some(CastingEnum::Enum("geo_nodes_level_enum".into())),
        }];

        let district_filter = vec![WhereField {
            field: "category_id".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec!["019ec96a-28f6-7481-9b37-167193f35d1c".into()]),
            casting: Some(CastingEnum::Uuid),
        }];

        let subdistrict = vec![WhereField {
            field: "level".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec!["adm4".into()]),
            casting: Some(CastingEnum::Enum("geo_nodes_level_enum".into())),
        }];

        let work_location_filter_group = vec![WhereField {
            field: "category_id".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec!["019e95a5-f5f3-7dd8-952b-0ce16845d9bd".into()]),
            casting: Some(CastingEnum::Uuid),
        }];
        let work_location_filter = vec![WhereField {
            field: "tenant_id".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec![tenant_id.into()]),
            casting: Some(CastingEnum::Uuid),
        }];

        let organization_filter = vec![WhereField {
            field: "tenant_id".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec![tenant_id.into()]),
            casting: Some(CastingEnum::Uuid),
        }];

        let job_position_filter = vec![WhereField {
            field: "tenant_id".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec![tenant_id.into()]),
            casting: Some(CastingEnum::Uuid),
        }];

        let job_grade_filter = vec![WhereField {
            field: "tenant_id".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec![tenant_id.into()]),
            casting: Some(CastingEnum::Uuid),
        }];

        let job_level_filter = vec![WhereField {
            field: "tenant_id".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec![tenant_id.into()]),
            casting: Some(CastingEnum::Uuid),
        }];

        let employment_status_filter = vec![WhereField {
            field: "tenant_id".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec![tenant_id.into()]),
            casting: Some(CastingEnum::Uuid),
        }];

        let employee_filter = vec![WhereField {
            field: "tenant_id".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec![tenant_id.into()]),
            casting: Some(CastingEnum::Uuid),
        }];

        let onboarding_items_filter = vec![WhereField {
            field: "tenant_id".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec![tenant_id.into()]),
            casting: Some(CastingEnum::Uuid),
        }];

        let region_filter = vec![WhereField {
            field: "category_id".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec!["019eab84-3b62-70ee-8574-f13596ccce55".into()]),
            casting: Some(CastingEnum::Uuid),
        }];
        let masters = vec![
            MasterConfig {
                title: "Province".into(),
                fetcher_key: "MASTER_GEO".into(),
                filter: Some(province_filter),
                name_field: "name",
            },
            MasterConfig {
                title: "City".into(),
                fetcher_key: "MASTER_GEO".into(),
                filter: Some(city_filter),
                name_field: "name",
            },
            MasterConfig {
                title: "District".into(),
                fetcher_key: "MASTER_ITEMS".into(),
                filter: Some(district_filter),
                name_field: "name",
            },
            MasterConfig {
                title: "Work Location".into(),
                fetcher_key: "ATTEDANCE_LOCATION".into(),
                filter: Some(work_location_filter),
                name_field: "name",
            },
            MasterConfig {
                title: "Work Location GROUP".into(),
                fetcher_key: "MASTER_ITEMS".into(),
                filter: Some(work_location_filter_group),
                name_field: "name",
            },
            MasterConfig {
                title: "Organization".into(),
                fetcher_key: "ORGANIZATION".into(),
                filter: Some(organization_filter),
                name_field: "name",
            },
            MasterConfig {
                title: "Job Position".into(),
                fetcher_key: "JOB_POSITION".into(),
                filter: Some(job_position_filter),
                name_field: "position_name",
            },
            MasterConfig {
                title: "Region".into(),
                fetcher_key: "MASTER_ITEMS".into(),
                filter: Some(region_filter),
                name_field: "name",
            },
            MasterConfig {
                title: "Job Grade".into(),
                fetcher_key: "JOB_GRADE".into(),
                filter: Some(job_grade_filter),
                name_field: "name",
            },
            MasterConfig {
                title: "Job Level".into(),
                fetcher_key: "JOB_LEVEL".into(),
                filter: Some(job_level_filter),
                name_field: "name",
            },
            MasterConfig {
                title: "Employee".into(),
                fetcher_key: "EMPLOYEE".into(),
                filter: Some(employee_filter),
                name_field: "full_name",
            },
            MasterConfig {
                title: "Employment Status".into(),
                fetcher_key: "EMPLOYMENT_STATUS".into(),
                filter: Some(employment_status_filter),
                name_field: "name",
            },
            // MasterConfig {
            //     title: "Onboarding Items".into(),
            //     fetcher_key: "ONBOARDING_ITEMS".into(),
            //     filter: Some(onboarding_items_filter),
            //     name_field: "group_name",
            // },
            MasterConfig {
                title: "PRE ONBOARDING TEMPLATE".into(),
                fetcher_key: "ONBOARDING_TEMPLATE".into(),
                filter: Some(onboarding_items_filter),
                name_field: "group_name",
            },
            MasterConfig {
                title: "Stage Template".into(),
                fetcher_key: "STAGE_TEMPLATE".into(),
                filter: None,
                name_field: "template_name",
            },
            // MasterConfig {
            //     title: "Stage Template Detail".into(),
            //     fetcher_key: "STAGE_TEMPLATE_DETAIL".into(),
            //     filter: None,
            //     name_field: "name",
            // },
            // MasterConfig {
            //     title: "Requirement Stage Template".into(),
            //     fetcher_key: "REQUIREMENT_STAGE_TEMPLATE".into(),
            //     filter: None,
            //     name_field: "template_name",
            // },
            // MasterConfig {
            //     title: "Requirement Stage Template Detail".into(),
            //     fetcher_key: "REQUIREMENT_STAGE_TEMPLATE_DETAIL".into(),
            //     filter: None,
            //     name_field: "name",
            // },
            MasterConfig {
                title: "Form Field".into(),
                fetcher_key: "FORM_FIELD".into(),
                filter: None,
                name_field: "name",
            },
            // MasterConfig {
            //     title: "Form Template Detail".into(),
            //     fetcher_key: "FORM_TEMPLATE_DETAIL".into(),
            //     filter: None,
            //     name_field: "name",
            // },
        ];

        let master_sheet = workbook.add_worksheet().set_name("master_template")?;
        let master_fetches = masters.iter().map(|config| {
            let fetcher = self.fetcher.get(&config.fetcher_key);
            let filter = config.filter.clone();

            async move {
                let fetcher = fetcher?;
                fetcher.fetch(filter).await
            }
        });
        let master_data = try_join_all(master_fetches).await?;

        for (idx, (config, raw_data)) in masters.iter().zip(master_data.iter()).enumerate() {
            let rows: Vec<LookupRow> = raw_data
                .iter()
                .map(|row| LookupRow {
                    id: row.get("id").map(value_to_string).unwrap_or_default(),
                    name: row
                        .get(config.name_field)
                        .map(value_to_string)
                        .unwrap_or_default(),
                })
                .filter(|row| !row.id.is_empty() && !row.name.is_empty())
                .collect();

            // 3 kolom per tabel: ID (x), Name (x+1), Gap (x+2)
            let start_col = (idx * 3) as u16;

            write_lookup_table(master_sheet, start_col, &config.title, &rows)?;

            // Otomatis atur lebar untuk 2 kolom data tersebut
            master_sheet.set_column_width(start_col, 22)?;
            master_sheet.set_column_width(start_col + 1, 22)?;
        }

        // ===== Lookup dinamis dari example_value + dropdown di main_import =====
        drop(master_sheet);
        let dynamic_start = masters.len();
        let mut validation_ranges = Vec::new();

        for (header_index, example) in example_values.iter().enumerate() {
            if !matches!(
                example.value_type,
                Some(ValueType::Select | ValueType::MultipleSelect)
            ) {
                continue;
            }

            let Some(fetcher_name) = example
                .fetcher
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };

            let fetcher = self.fetcher.get(fetcher_name)?;
            let raw_data = fetcher.fetch(example.filter.clone()).await?;
            let rows: Vec<LookupRow> = raw_data
                .iter()
                .map(|row| LookupRow {
                    id: row.get("id").map(value_to_string).unwrap_or_default(),
                    name: lookup_display_value(row, fetcher_name),
                })
                .filter(|row| !row.id.is_empty() && !row.name.is_empty())
                .collect();

            if rows.is_empty() {
                continue;
            }

            let start_col = ((dynamic_start + header_index) * 3) as u16;
            {
                let master_sheet = workbook.worksheet_from_name("master_template")?;
                write_lookup_table(master_sheet, start_col, fetcher_name, &rows)?;
                master_sheet.set_column_width(start_col, 22)?;
                master_sheet.set_column_width(start_col + 1, 28)?;
            }

            let range_name = format!("example_options_{header_index}");
            let display_column = excel_column_name(start_col + 1);
            workbook.define_name(
                &range_name,
                &format!(
                    "='master_template'!${display_column}$3:${display_column}${}",
                    rows.len() + 2
                ),
            )?;
            validation_ranges.push((
                header_index,
                range_name,
                matches!(example.value_type, Some(ValueType::MultipleSelect)),
            ));
        }

        let main_sheet = workbook.worksheet_from_name("main_import")?;
        for (header_index, range_name, is_multiple_select) in validation_ranges {
            // Excel tidak mendukung native multi-select; multipleSelect tetap
            // diberi dropdown single-select agar opsi tetap terlihat. Nilai
            // multipleSelect dapat diisi manual sebagai "A,B".
            let mut validation =
                DataValidation::new().allow_list_formula(Formula::new(format!("={range_name}")));
            if is_multiple_select {
                // Excel's list validation sees "A,B" as one value and would
                // reject it. The importer splits this value by comma, so do
                // not block the valid multi-value input here.
                validation = validation.show_error_message(false);
            }
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

        workbook.save(file_path)?;

        Ok(())
    }
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
        "EMPLOYEE" => &["full_name", "name"],
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
