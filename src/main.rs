use anyhow::Result;
 // Tambahkan import ini untuk mengaktifkan try_init

use crate::boostrap::bootstrap;

mod app_state;
mod application;
mod boostrap;
mod config;
mod core;
mod domain;
mod errors;
mod infrastructure;
mod repo;
mod utils;

#[global_allocator]
static GLOBAL: tracing_tracy::client::ProfiledAllocator<std::alloc::System> =
    tracing_tracy::client::ProfiledAllocator::new(std::alloc::System, 100);

#[tokio::main]
async fn main() -> Result<()> {
    // Gunakan try_init() menggantikan init() agar jika sudah ada logger lain,
    //
    unsafe {
        let layout = std::alloc::Layout::new::<u8>();
        let ptr = std::alloc::GlobalAlloc::alloc(&GLOBAL, layout);
        if !ptr.is_null() {
            std::alloc::GlobalAlloc::dealloc(&GLOBAL, ptr, layout);
        }
    }
    //

    if let Err(e) = bootstrap().await {
        eprintln!("fatal error: {:?}", e);
        std::process::exit(1);
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    Ok(())
}
