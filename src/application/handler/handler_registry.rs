use crate::core::traits::{handler::JobHandler, strategy::registry::Registry};

pub type HandlerRegistry = Registry<dyn JobHandler>;
