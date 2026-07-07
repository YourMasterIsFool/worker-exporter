pub mod attedances;
pub mod employee;
mod types;

use crate::core::types::{CastingEnum, WhereField};

pub fn normalize_hris_filters(filters: Option<Vec<WhereField>>) -> Option<Vec<WhereField>> {
    filters.map(|items| {
        items
            .into_iter()
            .map(|mut field| {
                let is_string_id_column = field.field == "id" || field.field.ends_with("_id");

                if is_string_id_column {
                    field.casting = Some(CastingEnum::Varchar);
                }

                field
            })
            .collect()
    })
}
