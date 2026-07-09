use sqlx::{Postgres, QueryBuilder};

use crate::core::types::{CastingEnum, WhereField};

pub fn resolve_query_builder_with_where_fields<'a>(
    qb: &mut QueryBuilder<'a, Postgres>,
    optional_where_fields: &'a Option<Vec<WhereField>>,
) {
    if let Some(where_fields) = optional_where_fields {
        for filter in where_fields {
            let casting = format!(
                "::{}",
                filter
                    .casting
                    .as_ref()
                    .unwrap_or(&CastingEnum::Varchar)
                    .as_str()
            );

            if let Some(value) = filter.value.as_ref() {
                qb.push(format!(
                    " AND {} {} ${}",
                    filter.field, filter.operator, casting
                ));
                qb.push_bind(value);
            }

            if filter.values.is_some() {
                qb.push(format!(
                    " AND {} {} ${}",
                    filter.field, filter.operator, casting
                ));
            }
        }
    }
}
