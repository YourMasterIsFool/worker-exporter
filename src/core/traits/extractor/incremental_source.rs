use anyhow::Result;

//buat incremental extract
//example id>watermark atau pakai date juga bisa > cursor_date
#[async_trait::async_trait]
pub trait IncrementalSource {
    type Watermark: Clone;
    type Output;
    async fn fetch_since(&self, limit: i64) -> Result<(Vec<Self::Output>)>;
}
