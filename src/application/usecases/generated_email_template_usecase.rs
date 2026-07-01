use std::{path::Path, sync::Arc};

use anyhow::anyhow;
use rust_xlsxwriter::{Format, FormatAlign, FormatBorder, Workbook, Worksheet};

use crate::{
    application::fetch_source_regitry::DataFetcherJsonRegistry,
    core::types::{CastingEnum, WhereField},
};

pub struct GeneratedEmailTemplateUsecase {
    pub fetcher: Arc<DataFetcherJsonRegistry>,
}

/// Representasi satu baris lookup table (id + name)
struct LookupRow {
    id: String,
    name: String,
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

        let work_location_filter = vec![WhereField {
            field: "tenant_id".into(),
            operator: "=".into(),
            value: None,
            values: Some(vec![tenant_id.into()]),
            casting: Some(CastingEnum::Uuid),
        }];

        let master_geo_fetcher = self.fetcher.get("master_geo_fetcher")?;
        let master_province = master_geo_fetcher.fetch(Some(province_filter)).await?;
        let master_city = master_geo_fetcher.fetch(Some(city_filter)).await?;
        let work_location_fetcher = self.fetcher.get("hris_attendance_locations_fetcher")?;
        let work_location = work_location_fetcher
            .fetch(Some(work_location_filter))
            .await?;

        // TODO: sesuaikan pemetaan hasil fetch ke LookupRow sesuai struct hasil fetch kamu
        let province_rows: Vec<LookupRow> = master_province
            .iter()
            .map(|row| LookupRow {
                id: row.get("id").map(|f| f.to_string()).unwrap(),
                name: row.get("name").map(|f| f.to_string()).unwrap_or_default(),
            })
            .collect();

        let city_rows: Vec<LookupRow> = master_city
            .iter()
            .map(|row| LookupRow {
                id: row.get("id").map(|f| f.to_string()).unwrap_or_default(),
                name: row.get("name").map(|f| f.to_string()).unwrap_or_default(),
            })
            .collect();

        // Kalau work location juga dari fetcher lain, fetch di sini juga
        let work_location_rows: Vec<LookupRow> = work_location
            .iter()
            .map(|row| LookupRow {
                id: row.get("id").map(|f| f.to_string()).unwrap_or_default(),
                name: row.get("name").map(|f| f.to_string()).unwrap_or_default(),
            })
            .collect(); // ganti sesuai source-nya

        // ===== Sheet 2: master_template =====
        let master_sheet = workbook.add_worksheet().set_name("master_template")?;

        let title_format = Format::new()
            .set_bold()
            .set_font_size(14)
            .set_align(FormatAlign::Center)
            .set_align(FormatAlign::VerticalCenter);

        let subheader_format = Format::new()
            .set_bold()
            .set_border(FormatBorder::Thin)
            .set_align(FormatAlign::Center)
            .set_background_color("#D9D9D9");

        let cell_format = Format::new().set_border(FormatBorder::Thin);

        // Kolom: PROVINCE di A-B, gap di C, CITY di D-E, gap di F, WORK LOCATION di G-H
        write_lookup_table(
            master_sheet,
            0, // start col A
            "PROVINCE",
            &province_rows,
            &title_format,
            &subheader_format,
            &cell_format,
        )?;

        write_lookup_table(
            master_sheet,
            3, // start col D
            "CITY",
            &city_rows,
            &title_format,
            &subheader_format,
            &cell_format,
        )?;

        write_lookup_table(
            master_sheet,
            6, // start col G
            "WORK LOCATION",
            &work_location_rows,
            &title_format,
            &subheader_format,
            &cell_format,
        )?;

        // Atur lebar kolom biar rapi
        for col in [0u16, 1, 3, 4, 6, 7] {
            master_sheet.set_column_width(col, 22)?;
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
    title_format: &Format,
    subheader_format: &Format,
    cell_format: &Format,
) -> anyhow::Result<()> {
    let id_col = start_col;
    let name_col = start_col + 1;

    // Row 0: title, merged di 2 kolom (id + name)
    sheet
        .merge_range(0, id_col, 0, name_col, title, title_format)
        .map_err(|e| anyhow!("gagal merge title {title}: {e}"))?;

    // Row 1: subheader "id" / "<Title> Name"
    sheet.write_string_with_format(1, id_col, "id", subheader_format)?;
    sheet.write_string_with_format(
        1,
        name_col,
        &format!("{} Name", to_title_case(title)),
        subheader_format,
    )?;

    // Data mulai row 2
    for (i, row) in rows.iter().enumerate() {
        let r = (i + 2) as u32;
        sheet.write_string_with_format(r, id_col, &row.id, cell_format)?;
        sheet.write_string_with_format(r, name_col, &row.name, cell_format)?;
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
