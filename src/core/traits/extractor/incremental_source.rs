use anyhow::Result;

//buat incremental extract
//example id>watermark atau pakai date juga bisa > cursor_date
//
//

pub struct WhereField {
    pub field: String,
    pub operator: String,
    pub value: String,
}

#[async_trait::async_trait]
pub trait IncrementalSource {
    type Cursor: Clone;
    type Output;

    async fn fetch(
        &self,
        limit: i64,
        cursor: Option<Self::Cursor>,
        filters: Option<Vec<WhereField>>,
    ) -> Result<(Vec<Self::Output>)>;
}
