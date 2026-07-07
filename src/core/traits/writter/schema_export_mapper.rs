pub trait SchemaExportMapper<T, O> {
    fn map(&self, row: T) -> O;
    fn headers(&self) -> Vec<String>;
}
