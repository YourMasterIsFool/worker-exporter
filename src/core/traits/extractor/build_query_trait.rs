pub trait BuildQueryTrait<T> {
    fn query(&self) -> &'static str;
    fn table(&self) -> T;
}
