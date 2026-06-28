pub enum MasterIngestionTable {
    MasterItem,
    Geo,
}

impl MasterIngestionTable {
    pub fn table_name(&self) -> &str {
        match self {
            Self::MasterItem => "public.master_items",
            Self::Geo => "public.geo_nodes",
        }
    }
}
