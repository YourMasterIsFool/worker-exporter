CREATE INDEX IF NOT EXISTS master_item_dimensions_source_updated_at
    ON olap.master_item_dimensions (source_updated_at DESC);

CREATE INDEX IF NOT EXISTS geo_dimensions_source_updated_at
    ON olap.geo_dimensions (source_updated_at DESC);
