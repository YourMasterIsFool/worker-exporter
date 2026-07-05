pub trait BuildTableTrait<T> {
    fn table(&self) -> T;
    fn columns(&self) -> &'static [&'static str];
}

pub trait TableEnum {
    fn table_name(&self) -> &str;
}
