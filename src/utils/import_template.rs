use calamine::Data;

/// Normalizes a header for matching between import configuration and workbook.
/// Whitespace, case, underscores, punctuation, and a trailing required marker
/// (`*`) are ignored. This keeps legacy config headers such as
/// `position_name` compatible with Excel headers such as `Position Name*`.
pub fn normalize_import_header(value: &str) -> String {
    value
        .trim()
        .trim_end_matches('*')
        .trim()
        .chars()
        .filter(|character| character.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

/// Resolves each configured field header to its column index in the template.
/// An unmatched configuration header becomes `None` and maps to an empty cell.
pub fn resolve_template_column_indexes(
    header_row: &[Data],
    field_headers: &[&str],
) -> Vec<Option<usize>> {
    field_headers
        .iter()
        .map(|field_header| {
            let expected_header = normalize_import_header(field_header);
            header_row.iter().position(|template_header| {
                normalize_import_header(&template_header.to_string()) == expected_header
            })
        })
        .collect()
}

/// Reorders a template row into configuration-field order using resolved column
/// indexes. Missing configured headers or cells become `Data::Empty`.
pub fn resolve_template_row(row: &[Data], column_indexes: &[Option<usize>]) -> Vec<Data> {
    column_indexes
        .iter()
        .map(|column_index| {
            column_index
                .and_then(|index| row.get(index).cloned())
                .unwrap_or(Data::Empty)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{resolve_template_column_indexes, resolve_template_row};
    use calamine::Data;

    #[test]
    fn resolves_rows_by_header_not_configuration_position() {
        let headers = vec![
            Data::String("Min Salary".into()),
            Data::String("Work Location Group*".into()),
        ];
        let configured_headers = ["Work Location Group*", "Min Salary"];
        let indexes = resolve_template_column_indexes(&headers, &configured_headers);
        let row = vec![
            Data::String("300000".into()),
            Data::String("Jakarta".into()),
        ];

        assert_eq!(indexes, vec![Some(1), Some(0)]);
        assert_eq!(
            resolve_template_row(&row, &indexes),
            vec![
                Data::String("Jakarta".into()),
                Data::String("300000".into())
            ]
        );
    }

    #[test]
    fn matches_legacy_snake_case_config_to_excel_header() {
        let headers = vec![Data::String("Position Name*".into())];
        let indexes = resolve_template_column_indexes(&headers, &["position_name"]);

        assert_eq!(indexes, vec![Some(0)]);
    }
}
