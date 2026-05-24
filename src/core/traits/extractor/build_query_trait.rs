pub trait BuildTableTrait<T> {
    fn table(&self) -> T;
    fn columns(&self) -> &'static [&'static str];
}
