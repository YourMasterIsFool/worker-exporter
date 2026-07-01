use std::sync::Arc;

use crate::{core::traits::load::LoaderPersitence, infrastructure::db::ats_client::AtsClient};

pub struct AtsVacancyLoader {
    pub client: Arc<AtsClient>,
}

impl LoaderPersitence for AtsVacancyLoader {
    fn table(&self) -> &'static str {
        "vacancy.vacancy"
    }

    fn columns(&self) -> &'static [&'static str] {
        &[]
    }

    fn conflicted_columns(&self) -> &'static [&'static str] {
        &[]
    }
}
