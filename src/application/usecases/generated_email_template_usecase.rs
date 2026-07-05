use std::{path::Path, sync::Arc, vec};

use anyhow::anyhow;
use rust_xlsxwriter::{Format, FormatAlign, FormatBorder, Workbook, Worksheet};

use crate::{
    application::fetch_source_regitry::DataFetcherJsonRegistry,
    core::types::{CastingEnum, WhereField},
    infrastructure::ingestion::hris::employee::employee,
};

pub struct GeneratedEmailTemplateUsecase {
    pub fetcher: Arc<DataFetcherJsonRegistry>,
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
        let main_sheet = workbook.add_worksheet().set_name("main_import")?;
        let header_format = Format::new()
            .set_bold()
            .set_border(FormatBorder::Thin)
            .set_align(FormatAlign::Center);

        for (col, header) in headers.iter().enumerate() {
            main_sheet.write_string_with_format(0, col as u16, header, &header_format)?;
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
        ];

        let master_sheet = workbook.add_worksheet().set_name("master_template")?;

        // Loop otomatis
        for (idx, config) in masters.iter().enumerate() {
            let fetcher = self.fetcher.get(&config.fetcher_key)?;
            let raw_data = fetcher.fetch(config.filter.clone()).await?;

            let rows: Vec<LookupRow> = raw_data
                .iter()
                .map(|row| LookupRow {
                    id: row.get("id").map(|f| f.to_string()).unwrap_or_default(),
                    name: row
                        .get(config.name_field)
                        .map(|f| f.to_string())
                        .unwrap_or_default(),
                })
                .collect();

            // 3 kolom per tabel: ID (x), Name (x+1), Gap (x+2)
            let start_col = (idx * 3) as u16;

            write_lookup_table(master_sheet, start_col, &config.title, &rows)?;

            // Otomatis atur lebar untuk 2 kolom data tersebut
            master_sheet.set_column_width(start_col, 22)?;
            master_sheet.set_column_width(start_col + 1, 22)?;
        }

        workbook.save(file_path)?;

        Ok(())
    }
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
