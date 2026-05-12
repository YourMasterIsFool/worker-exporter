/// TransformData
///
/// transform data sebagai mengubah data extractor ke data loader/warehouse
///
/// # EXAMPLE
/// ```rust
///     
/// pub struct CandidatePoolExtractor;
///
/// impl TrasnformData for CanddiatePoolExtractor {
///     type Input =  ExampleEtity;
///     type Output = OutputEntity;
///
///
///     async fn transform(&self, items: &[Self::Input]) -> Vec<Self::Output>
///     {
///         ///extractor from ....
///         /// tranform to output
///         todo!("transform data")
///     }
/// }
///     
/// ```

pub trait TransformData {
    type Input;
    type Output;

    fn transform(&self, items: Vec<Self::Input>) -> Vec<Self::Output>;
}
