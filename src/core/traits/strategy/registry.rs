use std::sync::Arc;
use std::{any, collections::HashMap};

use anyhow::{self, bail};
use tracing::warn;

pub struct Registry<T: ?Sized> {
    pub maps: HashMap<String, Arc<T>>,
}
impl<T: ?Sized> Registry<T> {
    pub fn new() -> Self {
        Self {
            maps: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: &str, reg: Arc<T>) -> anyhow::Result<()> {
        if self.maps.contains_key(name) {
            bail!("{} already registered", name);
        }

        self.maps.insert(name.to_string(), reg);
        Ok(())
    }

    pub fn get(&self, name: &str) -> anyhow::Result<Arc<T>> {
        self.maps
            .get(name)
            .cloned() // Mengambil clone dari Arc pointer (ringan)
            .ok_or_else(|| {
                warn!("not registered {}", name);
                // Mengembalikan error, bukan mematikan program
                anyhow::anyhow!("Registry: {} tidak ditemukan", name)
            })
    }
}
