use std::{any::Any, collections::HashMap, future::Future, pin::Pin};

use crate::core::traits::{grpc::GrpcExecutor, strategy::registry::Registry};

pub struct GrpcWrapper<S, Req, Res> {
    pub service: S,
    pub _phantom: std::marker::PhantomData<(Req, Res)>,
}

#[async_trait::async_trait]
impl<S, Req, Res> GrpcExecutor for GrpcWrapper<S, Req, Res>
where
    // S adalah fungsi yang mengembalikan Future yang sudah di-Pin dan Send
    S: Fn(Req) -> Pin<Box<dyn Future<Output = Res> + Send>> + Send + Sync + 'static,
    Req: 'static + Send + Sync,
    Res: 'static + Send + Sync,
{
    async fn execute(&self, request: Box<dyn Any + Send>) -> anyhow::Result<Box<dyn Any + Send>> {
        // 1. Downcast request
        let req = *request
            .downcast::<Req>()
            .map_err(|_| anyhow::anyhow!("Tipe request tidak sesuai"))?;

        // 2. Jalankan service async dan tunggu hasilnya
        let res = (self.service)(req).await;

        // 3. Kembalikan hasil sebagai Box<dyn Any + Send>
        Ok(Box::new(res))
    }
}

pub struct GrpcRegistry {
    pub map: HashMap<String, Box<dyn GrpcExecutor>>,
}

impl GrpcRegistry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn register<S, Req, Res, F>(&mut self, name: String, executor: S)
    where
        S: Fn(Req) -> F + Send + Sync + 'static,
        F: Future<Output = Res> + Send + 'static,
        Req: 'static + Send + Sync,
        Res: 'static + Send + Sync,
    {
        // Wrapper closure untuk menyesuaikan signature trait
        let wrapped_service =
            move |req: Req| Box::pin(executor(req)) as Pin<Box<dyn Future<Output = Res> + Send>>;

        self.map.insert(
            name,
            Box::new(GrpcWrapper {
                service: wrapped_service,
                _phantom: std::marker::PhantomData,
            }),
        );
    }

    pub fn get(&self, name: &str) -> Option<&dyn GrpcExecutor> {
        self.map.get(name).map(|b| b.as_ref())
    }
}
