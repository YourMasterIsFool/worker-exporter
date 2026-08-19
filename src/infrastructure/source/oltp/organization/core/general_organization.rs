use std::sync::Arc;

use sqlx::{Postgres, QueryBuilder};
use tracing::instrument;

use crate::{
    core::{
        traits::etl::{fetch_data_json, DataFetcherJson},
        types::WhereField,
    },
    domain::oltp::organization::general_organization::GeneralOrganization,
    infrastructure::db::organization_client::OrganizationClient,
};

pub struct GeneralOrganizationSource {
    pub pool: Arc<OrganizationClient>,
}

fn normalize_filters(filters: Option<Vec<WhereField>>) -> Option<Vec<WhereField>> {
    filters.map(|filters| {
        filters
            .into_iter()
            .map(|mut filter| {
                if filter.field.trim_matches('"').eq_ignore_ascii_case("type") {
                    filter.field = "organization_type".to_owned();
                }
                filter
            })
            .collect()
    })
}

#[async_trait::async_trait]
impl DataFetcherJson for GeneralOrganizationSource {
    #[instrument(skip(self), fields(types = "general organization data fetcher"))]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let mut query = QueryBuilder::<Postgres>::new(
            r#"
            SELECT
                organization.id,
                organization.updated_at,
                organization.created_at,
                organization.deleted_at,
                organization.filterable_attribute,
                organization.keywords,
                organization.tenant_id,
                organization.group_id,
                organization.code,
                organization.organization_type,
                organization.name,
                organization.is_default,
                organization.permission_keys,
                organization.additional_fields
            FROM (
                SELECT
                    general.id,
                    general.updated_at,
                    general.created_at,
                    general.deleted_at,
                    general.filterable_attribute,
                    general.keywords,
                    general.tenant_id,
                    general.group_id,
                    general.code,
                    general."type" AS organization_type,
                    general.name,
                    general.is_default,
                    general.permission_keys,
                    COALESCE(
                        (
                            SELECT jsonb_object_agg(
                                fields.field_key,
                                jsonb_build_object(
                                    'id', fields.id,
                                    'field_type', fields.field_type::text,
                                    'value_text', fields.value_text,
                                    'value_number', fields.value_number,
                                    'value_id', fields.value_id,
                                    'meta', fields.meta,
                                    'reference_module', fields.reference_module,
                                    'value_bool', fields.value_bool,
                                    'value_date', fields.value_date
                                )
                            )
                            FROM core.general_organization_additional_fields AS fields
                            WHERE fields.parent_id = general.id
                              AND fields.deleted_at IS NULL
                        ),
                        '{}'::jsonb
                    ) AS additional_fields
                FROM core.general_organizations AS general
                WHERE general.deleted_at IS NULL
            ) AS organization
            "#,
        );

        fetch_data_json::<GeneralOrganization>(
            &mut query,
            normalize_filters(filters),
            &self.pool.pool,
            Some("name ASC"),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_filters;
    use crate::core::types::WhereField;

    #[test]
    fn maps_schema_type_filter_to_query_alias() {
        let filters = normalize_filters(Some(vec![WhereField {
            field: "type".to_owned(),
            operator: "=".to_owned(),
            value: Some("BRANCH".to_owned()),
            values: None,
            casting: None,
        }]))
        .unwrap();

        assert_eq!(filters[0].field, "organization_type");
    }
}
