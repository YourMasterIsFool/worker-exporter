pub trait TransformData {
    type Input;
    type Output;

    fn transform(&self, items: Vec<Self::Input>) -> Vec<Self::Output>;
}
