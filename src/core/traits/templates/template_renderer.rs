// buat template excel renderer
pub trait TemplateRenderer<T> {
    // buat template renderer
    // contoh
    // header:name, column
    fn headers(&self) -> Vec<String>;

    // buat render row nya per entity
    fn render_row(&self, item: &T) -> Vec<String>;
}
