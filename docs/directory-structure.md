# Struktur direktori

```text
worker-exporter/
├── src/
│   ├── bin/
│   │   ├── http.rs
│   │   ├── worker.rs
│   │   └── http_worker.rs
│   ├── application/
│   │   ├── handler/
│   │   ├── orchestrator/
│   │   ├── runner/
│   │   ├── datasets/
│   │   ├── presentation/http/
│   │   ├── grpcs/
│   │   ├── import/
│   │   ├── sync/
│   │   └── *registry.rs, dataset.rs, writter.rs
│   ├── core/
│   │   ├── config/
│   │   ├── traits/
│   │   └── types.rs
│   ├── domain/
│   │   └── logging/
│   ├── infrastructure/
│   │   ├── db/
│   │   ├── source/
│   │   ├── ingestion/
│   │   ├── loader/
│   │   ├── datasets/
│   │   ├── exporter/
│   │   ├── pipeline/
│   │   ├── proto/
│   │   └── redis.rs
│   ├── utils/
│   ├── boostrap.rs
│   ├── app_state.rs
│   └── lib.rs
├── migrations/
├── docs/
├── scripts/
├── Cargo.toml
├── build.rs
├── Dockerfile
└── Makefile.toml
```

## Root proyek

| Lokasi | Fungsi |
| --- | --- |
| `Cargo.toml` | Manifest crate, dependency, feature allocator/profiling, dan profile build. |
| `build.rs` | Mengompilasi proto facilitator dan ATS dengan `tonic-build`. |
| `Dockerfile` | Multi-stage build Rust; menghasilkan binary `worker`, `http`, dan `http_worker`. |
| `Makefile.toml` | Task database warehouse untuk migrasi, info, revert, dan reset. |
| `migrations/` | Migrasi SQL warehouse dengan pasangan `.up.sql` dan `.down.sql`. |
| `scripts/` | Hook Git untuk validasi dan persiapan commit. |
| `docs/` | Dokumentasi operasional dan arsitektur proyek. |

## Entrypoint dan composition root

| Berkas | Fungsi |
| --- | --- |
| `src/bin/http_worker.rs` | Entry point gabungan HTTP API dan worker. Default image Docker. |
| `src/bin/worker.rs` | Entry point worker Redis tanpa HTTP API. |
| `src/bin/http.rs` | Entry point HTTP API tanpa consumer worker. |
| `src/main.rs` | Binary package default; hanya bootstrap dan bukan service jangka panjang. |
| `src/boostrap.rs` | Merakit configuration, koneksi, client, registry, repository, use case, dan `AppState`. |
| `src/app_state.rs` | Dependency bersama untuk handler HTTP dan Redis (`orchestrator_registry`, gRPC, publisher, Redis manager, use case). |

## `src/application/`

Folder ini mengatur use case tanpa menjadi pemilik implementasi akses teknologi rendah.

| Lokasi | Fungsi |
| --- | --- |
| `handler/` | Loop consumer generik dan adaptor `ImportHandler`, `ExportHandler`, `SyncHandler`, serta `IngestionHandler`. |
| `orchestrator/` | Koordinasi flow level tinggi dan registry `IMPORT`, `EXPORT`, `SYNC`. |
| `runner/` | Loop batching/cursor untuk export dan sync. |
| `datasets/` | Implementasi dataset export, import, dan sync; termasuk mapper reusable yang memakai `src/utils/`. |
| `dataset.rs` | Pendaftaran export dataset, import dataset, dan sync dataset. |
| `fetch_source_regitry.rs` | Pendaftaran fetcher JSON yang dipakai untuk lookup/mapping import. |
| `writter.rs` | Factory writer `csv` dan `excel`. Ejaan `writter` mengikuti kode saat ini. |
| `import/` | Pendaftaran loader temporary warehouse untuk import. |
| `sync/ats/` | Aggregator SYNC spesifik ATS vacancy. |
| `presentation/http/` | Handler Axum untuk endpoint yang diekspos HTTP. |
| `usecases/` | Use case HTTP seperti list logging dan generate template import. |
| `grpcs/` | Wrapper client gRPC untuk facilitator S3 dan ATS decryptor. |
| `pipelines/` | Kontrak dan registry pipeline import/ingestion; sebagian jalur lama belum aktif. |

## `src/core/`

`core` berisi bahasa bersama aplikasi. Ia tidak seharusnya mengetahui detail Redis, SQLx, CSV, atau fasilitas eksternal tertentu.

| Lokasi | Fungsi |
| --- | --- |
| `config/config.rs` | Membaca seluruh environment aplikasi. |
| `config/database_manager.rs` | Membuat lima `PgPool`. |
| `traits/consumer.rs` | Kontrak consume dan ACK. |
| `traits/orchestrator.rs` | Kontrak orchestrator dan payload job export/import/sync. |
| `traits/dataset_trait.rs` | Kontrak dataset export/import/sync. |
| `traits/load.rs` | Kontrak loader warehouse, import, sync, dan bulk loader. |
| `traits/strategy/registry.rs` | Registry generic berbasis key string. |
| `traits/writter/` | Kontrak writer berkas export. |
| `types.rs` | Tipe lintas layer, misalnya `WhereField`, casting, hasil import, dan row export. |

## `src/utils/`

Utility di sini berisi perilaku lintas dataset yang tidak bergantung pada source atau tabel
bisnis tertentu. Dataset tetap memiliki aturan alur dan mapping domainnya sendiri.

| Berkas | Fungsi |
| --- | --- |
| `fetcher.rs` | Filter fetcher, normalisasi cache key, single/batch lookup, dan logging cache. |
| `json_value.rs` | Resolusi JSON path, flatten array, keyword, unique values, dan index row. |
| `custom_field.rs` | Resolusi nilai custom field EAV serta grouping/index berdasarkan parent dan field key. |
| `export_resolver.rs` | Resolusi column path terhadap beberapa source JSON dan format nilai export. |
| `incremental_cursor_resolver.rs` | Resolusi cursor incremental untuk query. |
| `load_query.rs`, `query_builder.rs` | Penyusunan query/filter berdasarkan `WhereField`. |
| `value.rs` | Konversi nilai Excel/Calamine ke JSON dan formatting nilai. |

## `src/domain/`

Saat ini domain yang paling matang adalah `logging/`:

- `entity.rs` berisi `LoggingEntity`, `LoggingStatus`, dan `LoggingType`.
- `repo.rs` mendefinisikan kontrak `LoggingRepo`.

Subfolder `oltp/` dan `snapshots/` menjadi tempat model bisnis untuk data sumber/tujuan, seperti ATS dan organization.

## `src/infrastructure/`

| Lokasi | Fungsi |
| --- | --- |
| `redis.rs` | Implementasi Redis Stream consumer (`XREADGROUP`/`XACK`) dan publisher (`XADD`). |
| `db/` | Wrapper client untuk setiap database PostgreSQL. |
| `source/oltp/` | Pembaca incremental dari ATS dan organization OLTP. |
| `source/warehouse/temporary/` | Pembaca data temporary warehouse, dipakai terutama oleh SYNC. |
| `ingestion/` | Fetcher dari ATS, organization, master, HRIS, atau MySQL. |
| `loader/warehouse/temporary/` | Penulis hasil import ke tabel temporary. |
| `loader/warehouse/snapshots/` | Loader snapshot warehouse. |
| `loader/oltp/ats/bulk_sync/` | Bulk loader untuk sinkronisasi ke ATS. |
| `datasets/` | Modul placeholder/kompatibilitas infrastructure; implementasi dataset aktif berada di `src/application/datasets/`. |
| `exporter/` | Implementasi CSV dan XLSX writer. |
| `pipeline/ats/` | Pipeline import ATS yang terdaftar. |
| `proto/` | Definisi `.proto` yang dibangun pada waktu kompilasi. |

## File yang dihasilkan saat runtime

| Lokasi | Isi | Catatan |
| --- | --- | --- |
| `exports/` | CSV/XLSX sementara hasil EXPORT. | Tidak dihapus otomatis setelah upload pada implementasi saat ini. |
| `target/` | Artefak build Cargo. | Diabaikan Git. |

`exports/` diabaikan oleh Git. Berikan permission tulis kepada user/container yang menjalankan aplikasi.
