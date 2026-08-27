# Buku Pegangan Mini: Import & Export (Bahasa Bayi)

Dokumen panjang ada di bawah. Kalau baru mulai, baca bagian kecil ini dulu.

## Kamus bayi

| Kata | Artinya |
| --- | --- |
| Excel | Makanan yang mau kita baca. |
| Dataset | Otak yang membaca Excel dan menyusun datanya. |
| Loader warehouse | Tangan yang menyimpan hasil baca ke tabel sementara. |
| Fetcher | Mata yang mencari pilihan, misalnya nama Position lalu dapat ID-nya. |
| Sync | Kurir yang memindahkan data dari warehouse ke ATS/OLTP. |
| Bulk loader OLTP | Tangan terakhir yang menyimpan data ke ATS. |
| Registry | Buku alamat: nama di config harus didaftarkan di sini. |

Jalan datanya begini:

```text
Excel -> Import Dataset -> Import Pipeline -> Loader Warehouse -> tabel temporary
                                                          |
                                               (auto_sync: true)
                                                          v
temporary source -> Sync Dataset -> Sync Aggregator -> Bulk Loader OLTP -> ATS
```

## Kalau mau buat import baru

Contoh nama fitur: `MY_FEATURE`. Jangan mulai dari nol; salin pola vacancy yang sudah hidup.

1. **Buat tempat tidur data sementara (loader warehouse).**

   - File contoh: `src/infrastructure/loader/warehouse/temporary/ats/warehouse_ats_vacancy_temporary.rs`
   - File baru biasa diletakkan di: `src/infrastructure/loader/warehouse/temporary/<domain>/warehouse_<nama>_temporary.rs`
   - Implement trait `ImportLoadPersitence`.
   - Bila membuat tabel/kolom baru, buat migration di `migrations/` juga.
   - Daftarkan nama loader di `src/application/import/import_registry.rs`.

   Contoh nama yang dipakai config:

   ```rust
   "WAREHOUSE_MY_FEATURE_TEMPORARY"
   ```

2. **Buat otak pembaca Excel (import dataset).**

   - File contoh: `src/application/datasets/import/ats/lsi_import_vacancy_v1.rs`
   - File baru: `src/application/datasets/import/<domain>/<nama>_import_v1.rs`
   - Implement trait `ImporterDatasetV1` dari `src/core/traits/dataset_trait.rs`.
   - Masukkan module ke `src/application/datasets/import/<domain>/mod.rs`.
   - Daftarkan nama dataset di `src/application/dataset.rs`, fungsi `build_importer_dataset_registry_v1`.

   Contoh nama dataset:

   ```rust
   "MY_FEATURE_IMPORT_V1"
   ```

3. **Pakai pipeline yang sudah ada bila alurnya biasa.**

   Untuk import ke temporary warehouse, cukup pakai:

   ```text
   src/infrastructure/pipeline/ats/vacancy_import_pipeline.rs
   pipeline config: "vacancy_import_pipeline"
   ```

   Pipeline ini memilih loader dari `import_config.loader`. Buat pipeline baru hanya bila cara menyimpannya memang berbeda. Jika membuat baru, daftarkan di `src/application/pipelines/pipeline_import_registry.rs`.

4. **Buat fetcher jika Excel perlu memilih data lain.**

   Misalnya user mengetik `Store A`, tetapi database butuh UUID `work_location_id`.

   - Source fetcher: `src/infrastructure/source/oltp/<domain>/...` atau `src/infrastructure/ingestion/...`
   - Implement trait `DataFetcherJson`.
   - Daftarkan nama fetcher di `src/application/fetch_source_regitry.rs`.
   - Nama itu dipakai oleh `fields[].fetcher` di tenant config.

   Contoh nyata: `JOB_POSITION`, `GENERAL_ORGANIZATION`, `EMPLOYEE`, dan `REQUIREMENT_TEMPLATE`.

5. **Isi tenant config. Ini tombol yang menyambungkan semuanya.**

   Untuk vacancy, nama yang benar adalah:

   ```json
   {
     "orchestrator_name": "IMPORT",
     "sync_config": {
       "chunk": 100,
       "sync_aggregate": "LSI_VACANCY_SYNC"
     },
     "import_config": {
       "dataset": "LSI_IMPORT_VACANCY_V1",
       "loader": "WAREHOUSE_ATS_VACANCY_TEMPORARY",
       "pipeline": "vacancy_import_pipeline",
       "master_table": "warehouse.ats_vacancy_temporary",
       "primary_column": "id",
       "auto_sync": true,
       "start_row": 2,
       "fields": []
     }
   }
   ```

   Isi `dataset`, `loader`, dan `pipeline` harus **persis sama** dengan nama yang didaftarkan di registry. Huruf besar-kecil juga penting.

### Cara menulis satu kolom config

**Kolom teks biasa**:

```json
{
  "column": "warehouse.ats_vacancy_temporary:name",
  "header": "Position Alias*",
  "is_child": false,
  "is_custom": false,
  "type_data": "TEXT",
  "template": { "value": "Contoh", "value_type": "text" }
}
```

**Kolom pilihan yang masuk ke custom field**: Excel menampilkan `name`, sistem menyimpan `id` dan `meta`.

```json
{
  "column": "warehouse.ats_vacancy_custom_field:workLocationGroupId",
  "header": "Work Location Group*",
  "select": "warehouse.ats_vacancy_custom_field:workLocationGroupId:meta:name",
  "fetcher": "GENERAL_ORGANIZATION",
  "fetcher_key": "id",
  "fetcher_field_key": "name",
  "is_child": true,
  "is_custom": true,
  "type_data": "UUID",
  "template": {
    "fetcher": "GENERAL_ORGANIZATION",
    "value": null,
    "value_type": "select"
  }
}
```

Aturan kecil tapi penting:

- Custom field **wajib** punya `type_data`: `TEXT`, `UUID`, `NUMBER`, `BOOLEAN`, atau `DATE`. Jangan kosong; ATS akan menolak enum kosong.
- Untuk menampilkan nama custom field, pakai `select` dengan bentuk `table:field_key:meta:name`.
- `DATE` menerima `2026-01-10` dan `2026/01/10` dari Excel.
- Jangan membuat `field_key` yang sama dua kali dalam `fields`; cukup ubah field yang sudah ada.

## Kalau import tadi harus masuk ke ATS (sync)

Untuk vacancy, semua bagian ini sudah ada. Jika membuat entity baru, buat pasangan yang sama.

1. **Temporary source** supaya sync bisa membaca warehouse.

   - File contoh: `src/infrastructure/source/warehouse/temporary/ats/ats_vacancy_temporary.rs`
   - Daftarkan di `src/application/ingestion/source_registry.rs`.
   - Jika perlu dibaca sebagai child/fetcher juga, daftarkan di `src/application/fetch_source_regitry.rs`.

2. **Sync dataset** untuk membentuk data per tabel.

   - File contoh: `src/application/datasets/import/lsi_vacancy_sync_dataset.rs`
   - Implement trait `SyncDataset`.
   - Daftarkan di `src/application/dataset.rs`, fungsi `build_sync_dataset_registry`.

3. **Sync aggregator** untuk memilih source, dataset, dan urutan bulk loader.

   - File contoh: `src/application/sync/ats/vacancy.rs`
   - Implement trait `SyncAggregator`.
   - Daftarkan di `src/application/sync_aggregator_registry.rs`.
   - Isi nama aggregator itu pada `sync_config.sync_aggregate`.

4. **Bulk loader OLTP** untuk memasukkan data ke tabel ATS.

   - Folder: `src/infrastructure/loader/oltp/ats/bulk_sync/`
   - Contoh: `bulk_vacancy.rs`, `bulk_custom_field.rs`, dan `bulk_pic.rs`.
   - Implement trait `BulkLoaderTrait`.
   - Daftarkan di `src/application/bulk_loader_registry.rs`.
   - Masukkan key loader itu ke daftar `LOADERS` di sync aggregator.

## Kalau mau buat export baru

Versi kecilnya begini:

1. Buat dataset di `src/application/datasets/export/<domain>/<nama>_dataset.rs` dan implement `Dataset`.
2. Daftarkan dataset di `src/application/dataset.rs`, fungsi `build_dataset_registry`.
3. Jika dataset perlu relasi/pilihan tambahan, buat fetcher lalu daftar di `src/application/fetch_source_regitry.rs`.
4. Tenant config mengisi `export_config.dataset` dengan key dataset tadi dan memilih `format`: `csv`, `excel`, atau `excel_rs`.

```json
{
  "orchestrator_name": "EXPORT",
  "export_config": {
    "dataset": "MY_FEATURE_EXPORT",
    "format": "excel_rs",
    "chunk": 100,
    "fields": []
  }
}
```

## Checklist bayi sebelum bilang selesai

- [ ] Loader warehouse ada dan sudah terdaftar.
- [ ] Dataset implement trait yang benar dan sudah terdaftar.
- [ ] Fetcher ada dan sudah terdaftar bila ada kolom `fetcher`.
- [ ] Nama `dataset`, `loader`, `pipeline`, `fetcher`, dan `sync_aggregate` di config sama persis dengan registry.
- [ ] Semua custom field punya `type_data`.
- [ ] Jika `auto_sync: true`, temporary source, sync dataset, sync aggregator, dan bulk loader OLTP sudah tersambung.
- [ ] Jalankan `cargo check`, lalu coba satu baris Excel dulu.

---

# Worker Exporter — Architecture & Handover

Dokumen ini adalah panduan handover end-to-end untuk runtime `worker-exporter` per 25 Agustus 2026. Isi diambil dari implementasi yang aktif di `src/`, bukan dari rancangan ideal atau dokumentasi lama.

## Daftar isi

1. [Ringkasan sistem](#ringkasan-sistem)
2. [Runtime dan dependency](#runtime-dan-dependency)
3. [Kontrak job dan Redis Streams](#kontrak-job-dan-redis-streams)
4. [Registry aktif](#registry-aktif)
5. [Filter, operator, dan casting](#filter-operator-dan-casting)
6. [Flow EXPORT](#flow-export)
7. [Flow IMPORT](#flow-import)
8. [Flow SYNC](#flow-sync)
9. [Flow INGESTION](#flow-ingestion)
10. [Flow ROLLBACK](#flow-rollback)
11. [HTTP controller](#http-controller)
12. [Logging dan status](#logging-dan-status)
13. [Menambah kemampuan baru](#menambah-kemampuan-baru)
14. [Operasional dan troubleshooting](#operasional-dan-troubleshooting)
15. [Checklist handover](#checklist-handover)

## Ringkasan sistem

`worker-exporter` menerima job asynchronous melalui Redis Streams. Handler memilih orchestrator berdasarkan stream, orchestrator menyusun flow, dan registry memilih implementasi source, dataset, loader, pipeline, atau writer berdasarkan key string dari config.

```mermaid
flowchart LR
    Producer[Producer / HTTP API] --> Redis[(Redis Streams)]
    Redis --> Handler[Stream Handler]
    Handler --> Registry[Orchestrator Registry]
    Registry --> Orchestrator[Orchestrator]
    Orchestrator --> Source[(OLTP / Warehouse)]
    Orchestrator --> Dataset[Dataset / Mapper]
    Orchestrator --> Loader[Loader / Writer]
    Loader --> Target[(Warehouse / ATS)]
    Orchestrator --> Facilitator[Facilitator gRPC]
    Orchestrator --> Logging[(Warehouse loggings)]
```

Flow utama:

| Flow | Stream | Hasil |
| --- | --- | --- |
| EXPORT | `stream:export` | CSV/XLSX, upload facilitator, logging, event email |
| IMPORT | `stream:import` | Excel dipetakan ke temporary warehouse dan import entity state |
| SYNC | `stream:sync` | Temporary warehouse di-upsert ke ATS dalam transaksi per batch |
| INGESTION | `stream:ingestion` | Delta source di-load ke OLAP/warehouse |
| ROLLBACK | `stream:rollback` | Record ATS milik satu `logging_id` dihapus secara transaksional |

Titik masuk utama:

| Binary | Fungsi |
| --- | --- |
| `http_worker` | HTTP server dan seluruh worker; mode deployment default |
| `worker` | Seluruh worker tanpa HTTP server |
| `http` | HTTP server tanpa worker |
| binary default `worker-exporter` | Hanya bootstrap lalu berhenti; jangan dipakai sebagai service |

Composition root berada di [`src/boostrap.rs`](src/boostrap.rs). File ini membuka koneksi, menjalankan migration warehouse, membangun seluruh registry, lalu membuat `AppState`.

### Struktur direktori

```text
src/
├── bin/                         # entrypoint http, worker, http_worker
├── application/
│   ├── handler/                 # consumer loop per stream
│   ├── orchestrator/            # koordinasi flow
│   ├── runner/                  # batching/cursor export dan sync
│   ├── datasets/                # mapper export/import/sync
│   ├── presentation/http/       # controller Axum
│   ├── import/                  # registry loader import
│   ├── sync/                    # aggregator sync
│   └── *registry.rs             # composition registry
├── core/
│   ├── traits/                  # kontrak source/dataset/loader/orchestrator
│   └── types.rs                 # tipe lintas flow
├── domain/logging/              # entity dan repository logging
├── infrastructure/
│   ├── db/                      # database clients
│   ├── source/                  # source incremental
│   ├── ingestion/               # fetcher JSON
│   ├── loader/                  # warehouse/OLTP loaders
│   ├── exporter/                # CSV/XLSX writers
│   ├── pipeline/                # implementasi pipeline import
│   ├── proto/                   # facilitator dan ATS proto
│   └── redis.rs                 # XADD, XREADGROUP, XACK
└── utils/                        # resolver/filter/cache/query reusable
```

Catatan penamaan: `boostrap`, `writter`, `dynamci_dataset_v1`, dan `emplyoee_statuses` adalah typo yang sudah menjadi bagian dari path/API internal. Jangan melakukan rename sambil menambah fitur lain.

## Runtime dan dependency

### Environment wajib

`HOST` dan `PORT` memiliki default. Semua nilai lain wajib tersedia saat startup, meskipun job tertentu tidak menggunakannya.

```dotenv
HOST=0.0.0.0
PORT=8080

REDIS_URL=redis://127.0.0.1:6379
WAREHOUSE_DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/warehouse
ORGANIZATION_DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/organization
ATS_DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/ats
MASTER_DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/master
HRIS_DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/hris
FACILITATOR_GRPC_URL=http://facilitator:50051
ATS_GRPC_URL=http://ats:50052
```

Startup akan gagal jika Redis, salah satu dari lima PostgreSQL pool, atau URL gRPC tidak valid. Migration pada folder `migrations/` dijalankan otomatis hanya ke `WAREHOUSE_DATABASE_URL` melalui `sqlx::migrate!`.

### Menjalankan lokal

```bash
cargo check
cargo run --bin http_worker
```

Verifikasi:

```bash
curl http://127.0.0.1:8080/healthz
```

Memisahkan proses:

```bash
# terminal 1
cargo run --bin http

# terminal 2
cargo run --bin worker
```

Jangan menjalankan `http_worker` dan `worker` bersamaan kecuali memang ingin menambah concurrency pada consumer group yang sama.

### Worker concurrency

`http_worker` dan `worker` menyalakan:

- 1 consumer ingestion;
- masing-masing 3 consumer export, import, sync, rollback, dan aggregate.

Consumer membaca maksimal 10 message per panggilan dengan `BLOCK 100ms`.

### Dependency eksternal per flow

| Dependency | Penggunaan |
| --- | --- |
| Redis | job streams, email event, auto-sync, aggregate event |
| Warehouse PostgreSQL | migration, logging, temporary import, OLAP |
| ATS PostgreSQL | export ATS, sync target, rollback |
| Organization PostgreSQL | fetcher organization/job metadata |
| Master PostgreSQL | master item dan geo fetcher/ingestion |
| HRIS PostgreSQL | employee/status/attendance fetcher |
| Facilitator gRPC | download file import dan upload/download file export |
| ATS decryptor gRPC | decrypt field dynamic export |

## Kontrak job dan Redis Streams

### Envelope umum

Payload worker mengikuti `OrchestratorJob` di [`src/core/traits/orchestrator.rs`](src/core/traits/orchestrator.rs).

```json
{
  "tenant_id": "TENANT_ID",
  "title": "Judul job",
  "email": "requester@example.com",
  "actor_id": "ACTOR_ID",
  "permissions_key": null,
  "orchestrator_name": "EXPORT",
  "export_config": null,
  "dynamic_export_config": null,
  "ingestion_config": null,
  "import_config": null,
  "sync_config": null,
  "rollback_config": null,
  "aggregate_config": null,
  "filters": null
}
```

Field wajib pada root adalah `tenant_id`, `email`, `actor_id`, dan `orchestrator_name`. Field config lain bertipe optional dan boleh dihilangkan jika tidak dipakai flow tersebut.

`type` bukan bagian dari `OrchestratorJob`. Field ini hanya dipakai oleh `POST /test-import` untuk memilih stream dan diabaikan saat payload dibaca worker.

### Routing sebenarnya

Routing ditentukan oleh stream, bukan nilai `orchestrator_name`:

| Stream | Handler meminta key orchestrator |
| --- | --- |
| `stream:export` | `EXPORT`, atau `DYNAMIC_EXPORT` jika `dynamic_export_config` ada |
| `stream:import` | `IMPORT`, atau `DYNAMIC_IMPORT` jika dataset adalah `DYNAMIC_IMPORT_DATASET` |
| `stream:sync` | `SYNC` |
| `stream:ingestion` | `ingestion` |
| `stream:rollback` | `ROLLBACK` |
| `stream:aggregate` | `AGGREGATE` |

Gunakan nilai `orchestrator_name` yang konsisten untuk audit, tetapi jangan mengandalkannya untuk memindahkan job antar-flow.

### Publish langsung ke Redis

Setiap stream entry harus memiliki field bernama `payload` yang berisi JSON string.

```bash
redis-cli -u "$REDIS_URL" \
  XADD stream:export '*' payload "$(jq -c . export-job.json)"
```

Field selain `payload` tidak dibaca consumer.

### Delivery dan ACK

Consumer melakukan `XACK` setelah setiap message, termasuk ketika:

- JSON gagal dideserialisasi;
- registry key tidak ditemukan;
- orchestrator mengembalikan error.

Tidak ada retry otomatis, DLQ, `XAUTOCLAIM`, atau recovery pending message. Perbaiki penyebab lalu publish job baru. Consumer group baru dibuat dengan start ID `$`, sehingga message lama sebelum group dibuat tidak ikut dibaca.

## Registry aktif

Registry memakai exact string match dan case-sensitive.

Konvensi untuk key baru adalah `UPPER_SNAKE_CASE`, misalnya `WAREHOUSE_VACANCY_SOURCE`, `JOB_LEVEL_FETCHER`, dan `WAREHOUSE_VACANCY_LOADER`. Key lowercase pada daftar aktif adalah legacy; jangan rename langsung karena config yang sudah tersimpan akan gagal. Bila perlu migrasi, register alias uppercase, pindahkan seluruh config, lalu hapus alias lama pada release terpisah.

### Registry flow utama

| Registry | Key aktif |
| --- | --- |
| Orchestrator | `IMPORT`, `DYNAMIC_IMPORT`, `EXPORT`, `DYNAMIC_EXPORT`, `SYNC`, `ROLLBACK`, `AGGREGATE`, `ingestion` |
| Writer | `csv`, `excel`, `excel_rs` |
| Export dataset | `lsi_ats_candidate_dataset`, `CANDIDATE_DATASET`, `lsi_reference_check_dataset`, `DYNAMIC_EXPORT_DATASET`, `APPLICANT_DATASET` |
| Import dataset v1 | `DYNAMIC_DATASET_V1`, `DYNAMIC_IMPORT_DATASET`, `LSI_IMPORT_VACANCY_V1` |
| Import pipeline | `CONFIG_IMPORT_PIPELINE`, `vacancy_import_pipeline` |
| Import loader | `WAREHOUSE_ATS_VACANCY_TEMPORARY`, `WAREHOUSE_DYNAMIC_TEMPORARY_LOADER` |
| Sync aggregator | `LSI_VACANCY_SYNC`, `CONFIG_SYNC_PIPELINE` |
| Sync dataset | `LSI_VACANCY_SYNC_DATASET` |
| Rollback pipeline | `CONFIG_ROLLBACK_PIPELINE` |

`DYNAMIC_DATASET_V1` dan `DYNAMIC_IMPORT_DATASET` memakai mapper generic yang sama. Key kedua hanya membuat routing eksplisit menuju wrapper `DYNAMIC_IMPORT`.

### Source registry

Key source incremental aktif:

- OLTP: `oltp_tenants`, `oltp_ats_candidate`, `oltp_ats_vacancy`, `oltp_ats_candidate_client_field`, `oltp_ats_applicant_bucket`, `oltp_ats_experience_source`, `oltp_ats_education_source`.
- Warehouse temporary: `warehouse_ats_vacancy_temporary`, `warehouse_ats_vacancy_stage_temporary`, `warehouse_ats_vacancy_requirement_temporary`, `warehouse_ats_vacancy_custom_field_temporary`, `warehouse_ats_vacancy_pic_temporary`.
- Master: `master_items`, `master_geo`.

Tidak semua source mendukung semua method trait. Untuk flow ingestion berbasis `updated_at`, lihat [pasangan ingestion yang aktif](#pasangan-ingestion-yang-aktif).

### Data fetcher registry

Fetcher digunakan oleh import config, template generator, dan beberapa dataset static:

| Domain | Key |
| --- | --- |
| ATS candidate | `ats_candidate_fetcher`, `ats_candidate_custom_field_fetcher`, `ats_candidate_field_fetcher`, `ats_education_fetcher`, `ats_experience_fetcher` |
| ATS applicant/vacancy | `ats_applicant_bucket_fetcher`, `ats_applicant_snapshot_fetcher`, `ats_vacancy_fetcher`, `OLTP_VACANCY_CUSTOM_FIELD`, `VACANCY_CUSTOM_FIELD` |
| ATS form/process | `STAGE_TEMPLATE`, `REQUIREMENT_STAGE_TEMPLATE`, `STAGE_TEMPLATE_DETAIL`, `REQUIREMENT_STAGE_TEMPLATE_DETAIL`, `FORM_TEMPLATE`, `REQUIREMENT_TEMPLATE`, `FORM_TEMPLATE_DETAIL`, `FORM_FIELD`, `FORM`, `FORM_QUESTION`, `FORM_QUESTION_OPTION`, `FORM_RESPONSE`, `FORM_RESPONSE_ANSWER`, `ats_reference_check_fetcher`, `ats_reference_check_referee_fetcher` |
| Warehouse temporary | `WAREHOUSE_ATS_VACANCY_TEMPORARY`, `WAREHOUSE_ATS_VACANCY_CUSTOM_FIELD`, `WAREHOUSE_ATS_VACANCY_PIC`, `WAREHOUSE_ATS_VACANCY_STAGE`, `WAREHOUSE_ATS_VACANCY_REQUIREMENT`, `WAREHOUSE_ATS_VACANCY_PUBLISH_ON` |
| Master | `MASTER_GEO`, `MASTER_ITEMS` |
| Organization | `JOB_POSITION`, `JOB_POSITION_CUSTOM_FIELD`, `JOB_POSITION_CUSTOM_FIELDS`, `ORGANIZATION`, `GENERAL_ORGANIZATION`, `JOB_GRADE`, `JOB_LEVEL`, `ONBOARDING_ITEMS`, `ONBOARDING_TEMPLATE` |
| HRIS | `ATTEDANCE_LOCATION`, `EMPLOYMENT_STATUS`, `EMPLOYEE` |

`ATTEDANCE_LOCATION` mengikuti typo registry yang ada. Gunakan persis seperti itu.

### Loader dan rollback key ATS

Bulk sync loader:

```text
ATS_VACANCY
ATS_VACANCY_CUSTOM_FIELD
ATS_VACANCY_PIC
ATS_VACANCY_STAGE
ATS_VACANCY_REQUIREMENT
ATS_VACANCY_PUBLISH_ON
```

Rollback loader:

```text
ATS_VACANCY_REQUIREMENT_ROLLBACK
ATS_VACANCY_STAGE_ROLLBACK
ATS_VACANCY_PUBLISH_ON_ROLLBACK
ATS_VACANCY_PIC_ROLLBACK
ATS_VACANCY_CUSTOM_FIELD_ROLLBACK
ATS_VACANCY_ROLLBACK
```

## Filter, operator, dan casting

`WhereField` dipakai untuk filter source/dataset. Bentuk JSON-nya:

```json
{
  "field": "tenant_id",
  "operator": "=",
  "value": "TENANT_UUID",
  "values": null,
  "casting": "Uuid"
}
```

Gunakan salah satu dari `value` untuk satu nilai atau `values` untuk banyak nilai. Jangan mengisi keduanya kecuali implementasi dataset yang dipakai memang mendokumentasikan prioritasnya.

### `casting` berbeda dengan `type_data`

| Field config | Fungsi | Contoh |
| --- | --- | --- |
| `fields[].type_data` | Mengubah cell Excel menjadi nilai target import | `"UUID"`, `"DATE"`, `"NUMBER"` |
| `fields[].casting` | Menentukan tipe filter ketika fetcher mencari lookup | `"Uuid"`, `"Varchar"`, `"Date"` |
| root `filters[].casting` | Menentukan cast filter yang diteruskan dataset static/source | `"Uuid"`, `"Date"` |

Contoh yang umum: Excel berisi nama `Job Level`, maka gunakan `casting: "Varchar"` untuk lookup `name`, tetapi `type_data: "UUID"` karena `fetcher_key: "id"` yang masuk ke `job_level_id` adalah UUID.

### Casting yang dapat dideserialisasi

Enum `CastingEnum` saat ini menerima nilai berikut:

| JSON | PostgreSQL cast yang dihasilkan | Penggunaan aman |
| --- | --- | --- |
| `"Uuid"` atau `"uuid"` | `uuid` | UUID ID, tenant/group ID, foreign key |
| `"Varchar"` atau `"varchar"` | `varchar` | lookup text/nama/email; default jika `casting` kosong |
| `"Date"` atau `"date"` | `date` | tanggal tanpa waktu |
| `"Number"` atau `"number"` | `number` | **jangan dipakai untuk fetcher saat ini**; PostgreSQL standar memakai `numeric`, bukan `number` |
| `{ "Enum": "schema.enum_type" }` | tipe string di dalam payload | hanya untuk kode internal yang terkontrol, misalnya bulk loader ATS |

`Enum` dipakai oleh bulk loader yang dikompilasi, misalnya `vacancy.vacancy_status_enum`. Jangan menerima nilai `Enum` dari config user: nama type dimasukkan ke SQL dan validasi whitelist `validate_cast_type()` belum dipanggil oleh `fetch_data_json`.

Walaupun terdapat daftar whitelist internal (`uuid`, `varchar`, `text`, `int4`, `int8`, `integer`, `bigint`, `numeric`, `boolean`, `date`, `timestamp`, `timestamptz`), daftar itu belum menjadi enforcement pada jalur fetcher config. Untuk config JSON saat ini, gunakan hanya `Uuid`, `Varchar`, dan `Date`. Tambahkan variant enum yang eksplisit sebelum membuka kebutuhan type lain seperti `numeric` atau `boolean`.

### Operator pada import fetcher dan source generic

Import tidak menerima `operator` langsung pada `fields[]`. Orchestrator memilih operator lookup berdasarkan `casting`:

| Casting | Operator yang dibuat import | Perilaku |
| --- | --- | --- |
| `Varchar` atau kosong | `ILIKE` | `fetch_data_json` menurunkan kedua sisi dan menjalankan pencocokan case-insensitive |
| `Uuid`, `Date`, `Number`, `Enum(...)` | `=` | exact match |

Saat lookup lebih dari satu token, helper fetcher meneruskan filter sebagai `column <operator> ANY($1::<cast>[])`. Kontrak aman untuk fetcher import adalah `ILIKE`/`=`. Jangan memakai `IN`, `NOT IN`, atau `IS NULL` pada jalur ini: `IN ANY(...)` bukan SQL PostgreSQL yang valid dan `IS NULL` tanpa `value`/`values` tidak dibangun oleh helper.

Source yang memakai `fetch_data_json` dapat menerima `WhereField` dari kode, tetapi `field`, `operator`, dan cast disusun ke SQL. Tidak ada allowlist operator aktif pada helper tersebut. Karena itu:

- untuk satu value, gunakan hanya `=`, `ILIKE`, `!=`, `<>`, `>`, `>=`, `<`, atau `<=` sesuai tipe kolom;
- untuk banyak values, gunakan `=` atau `ILIKE` agar menjadi `= ANY(...)` secara benar;
- gunakan filter hanya dari config admin yang tervalidasi; jangan expose sebagai parameter HTTP publik;
- source custom boleh menambah operator lain, tetapi harus membuat allowlist sendiri dan tetap memakai bind parameter untuk value.

Contoh fetcher lookup yang benar:

```json
{
  "fetcher": "JOB_LEVEL",
  "fetcher_field_key": "name",
  "fetcher_key": "id",
  "casting": "Varchar",
  "type_data": "UUID"
}
```

### Operator dynamic export

Dynamic export memvalidasi identifier dan memiliki allowlist operator berikut. `casting` diabaikan di flow ini; pembandingan scalar memakai `column::text`.

| Operator | Input yang diperlukan | Contoh |
| --- | --- | --- |
| `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=` | `value` | `{"field":"created_at","operator":">=","value":"2026-01-01"}` |
| `IS NULL`, `IS NOT NULL` | tanpa value | `{"field":"deleted_at","operator":"IS NULL"}` |
| `IN`, `NOT IN` | `values`, atau satu `value` | `{"field":"status","operator":"IN","values":["DRAFT","PUBLISHED"]}` |
| `ARRAY_OVERLAPS` atau `&&` | `values`, atau satu `value` | `{"field":"tenant_pool_keys","operator":"ARRAY_OVERLAPS","values":["TENANT_UUID"]}` |
| `JSONB_CONTAINS` atau `@>` | `values`, atau satu `value` | `{"field":"job_functions","operator":"JSONB_CONTAINS","values":["ENGINEERING"]}` |

`ARRAY_OVERLAPS` selalu membind input sebagai `text[]`; gunakan hanya pada kolom array text. `JSONB_CONTAINS` membuat JSON array dari input (`["READ_VACANCY"]`), sehingga dipakai untuk JSONB array, bukan object JSON arbitrer. Operator lain ditolak dengan error `dynamic export operator is not allowed`.

### Static export dan sync

Static export menyerahkan `filters` ke masing-masing dataset; dukungan operator bergantung implementasi dataset tersebut. Jangan menganggap semua static dataset memiliki kemampuan dynamic export. SYNC tidak memakai operator dari payload: legacy maupun config pipeline selalu memfilter staging dengan `logging_id = <id>` dan casting `Varchar`.

## Flow EXPORT

### End-to-end

```mermaid
flowchart LR
    Producer -->|stream:export| Handler[ExportHandler]
    Handler --> EO[ExportOrchestrator]
    EO --> Log[(loggings)]
    EO --> Dataset[Dataset Registry]
    Dataset --> Source[(Source DB)]
    EO --> Writer[CSV/XLSX Writer]
    Writer --> File[exports/message-id.ext]
    File --> S3[Facilitator upload_file]
    S3 --> Log
    EO --> Email[stream:email]
```

Urutan aktual:

1. Handler memilih `EXPORT` atau `DYNAMIC_EXPORT`.
2. Orchestrator menyimpan logging `EXPORT/PROCESSING`.
3. Writer dan dataset diambil dari registry.
4. `ExportRunner` menambah kolom `number`, membaca per `chunk`, menulis row, dan memperbarui cursor/progress.
5. File disimpan di `exports/`, kemudian dibaca ke memory untuk upload facilitator.
6. `download_url` dan `s3_key` dicatat.
7. Event email selalu dipublish ke `stream:email`.
8. Logging ditandai `COMPLETED`.

File lokal belum dihapus setelah upload. Gunakan ephemeral storage atau janitor untuk direktori `exports/`.

### Progress logging: total proses, chunk, dan cursor

[`src/application/orchestrator/dynamic_export_orchestrator.rs`](src/application/orchestrator/dynamic_export_orchestrator.rs) hanya memvalidasi adanya `dynamic_export_config`, lalu mendelegasikan seluruh pekerjaan ke [`src/application/orchestrator/export_orchestrator.rs`](src/application/orchestrator/export_orchestrator.rs). Karena itu static dan dynamic export memakai tabel logging serta `ExportRunner` yang sama.

Nama field aktual pada `loggings` adalah `processed_count` dan `total_count`; tidak ada field bernama `total_process`.

| Field logging | Nilai awal dynamic export | Perubahan selama export | Arti |
| --- | --- | --- | --- |
| `status` | `PROCESSING` | `COMPLETED` setelah upload sukses dan percobaan publish event email | lifecycle export |
| `total_count` | `COUNT(*)` source dengan tenant/filter/relation filter | tidak dihitung ulang | estimasi jumlah root row saat job dimulai |
| `processed_count` | `0` | diakumulasi setelah setiap batch berhasil ditulis | jumlah data row yang sudah masuk file, tanpa header `number` |
| `total_chunk` | `ceil(total_count / chunk)` bila dataset menyediakan count | tetap | estimasi jumlah batch |
| `current_chunk` | `0` | naik satu setelah setiap batch yang tracked berhasil ditulis | batch yang telah selesai |
| `cursor` | `NULL` untuk dynamic | cursor terakhir setelah setiap batch | checkpoint pagination; jangan dipakai untuk resume otomatis saat ini |
| `stream_id` | Redis message ID | tetap | lookup utama logging, juga dapat diganti ID logging pada repository |
| `config` | seluruh `OrchestratorJob` | tetap | payload audit/reproduksi job |
| `download_url`, `s3_key` | `NULL` | diisi setelah upload facilitator sukses | attachment hasil export |

Formula `total_chunk` dihitung untuk dynamic export dan static dataset yang mengimplementasikan `Dataset::count()`:

```text
total_chunk = (total_count + chunk - 1) / chunk
```

Contoh `total_count: 1_251` dan `chunk: 500` menghasilkan `total_chunk: 3`. Sesudah batch pertama, logging menjadi `processed_count: 500`, `current_chunk: 1`, dan `cursor` berisi ID cursor terakhir. Sesudah batch terakhir, `processed_count` seharusnya `1_251` dan `current_chunk: 3`.

`total_count` dynamic adalah `COUNT(*)` table source setelah source filter dan relation filter `EXISTS` diterapkan. Ia tidak menghitung ulang setelah relation dimuat, decrypt dijalankan, atau data source berubah di tengah job. Karena itu total/count chunk adalah snapshot progress, bukan jaminan absolut saat data aktif berubah.

Static dataset legacy yang belum mengimplementasikan `Dataset::count()` tetap diinisialisasi dengan `total_count: 0` dan `total_chunk: 0`; nilai tersebut berarti *tidak dihitung*, bukan selalu tidak ada data. `CANDIDATE_DATASET` mengimplementasikan count, sehingga static export kandidat juga mengisi `total_count`, `total_chunk`, dan `current_chunk` seperti dynamic export. `processed_count` dan `cursor` tetap diperbarui setiap batch pada seluruh dataset.

`ExportRunner` melakukan urutan berikut pada setiap batch yang tidak kosong:

1. mengambil data memakai cursor dan `chunk`;
2. menambah kolom nomor urut `number` hanya pada file;
3. menulis row ke writer;
4. menambah `processed_count`;
5. menyimpan `cursor` dan `processed_count` dalam satu update logging;
6. menyimpan `current_chunk` untuk dynamic export atau static dataset yang menyediakan count;
7. berhenti bila dataset mengembalikan `new_cursor: null`.

Jika hasil awal kosong, writer tetap menghasilkan file berisi header, progress tetap nol, kemudian job dapat menjadi `COMPLETED`. Untuk dynamic export, `chunk <= 0` ditolak sebelum logging dibuat. Static export belum memiliki validasi awal yang setara, sehingga config static wajib selalu memakai `chunk > 0`.

Gunakan query ini untuk memantau job yang sedang berjalan:

```sql
SELECT
    id,
    stream_id,
    status,
    total_count,
    processed_count,
    current_chunk,
    total_chunk,
    cursor,
    download_url,
    s3_key,
    error,
    created_at,
    completed_at
FROM loggings
WHERE stream_id = 'REDIS_MESSAGE_ID';
```

Persentase hanya bermakna bila `total_count > 0`:

```text
progress_percent = processed_count * 100.0 / total_count
```

Jangan menyimpan persentase ini ke database; hitung pada API/UI dari field logging. Saat `total_count = 0`, tampilkan `unknown` untuk static export dan `0 / 0` untuk dynamic export yang memang tidak menemukan data.

Known behavior penting:

- error saat menghitung dynamic total atau `chunk <= 0` terjadi sebelum logging dibuat;
- error setelah logging dibuat belum otomatis mengubah status menjadi `FAILED` atau mengisi `error`, sehingga job dapat tertinggal `PROCESSING`;
- `cursor` sudah disimpan, tetapi belum ada endpoint/worker resume yang melanjutkan file writer lama. Re-run job membuat file baru;
- bila `export_config` dan `dynamic_export_config` sama-sama dikirim, `fields`, `format`, template email, `redirected_url`, dan `chunk` dari `export_config` menggantikan nilai dynamic. Source/relations tetap dari `dynamic_export_config`. Untuk menghindari config campuran, kirim satu bentuk config yang jelas.

### Static export

Static export memakai implementasi `Dataset` yang sudah dikompilasi dan diregister. Config hanya memilih dataset, field, writer, dan chunk.

#### Config static export

```json
{
  "type": "export",
  "tenant_id": "TENANT_UUID",
  "title": "Candidate export",
  "email": "requester@example.com",
  "actor_id": "ACTOR_ID",
  "orchestrator_name": "EXPORT",
  "filters": null,
  "export_config": {
    "dataset": "lsi_ats_candidate_dataset",
    "format": "excel_rs",
    "chunk": 500,
    "fields": [
      {"name": "Candidate ID", "column": "candidate_id"},
      {"name": "Nama Lengkap", "column": "name"},
      {"name": "Email", "column": "email_address"}
    ],
    "template_code": "EMAIL_TEMPLATE_CODE",
    "structure_template_code": "EMAIL_STRUCTURE_CODE",
    "redirected_url": "https://app.example.com/download?key={{url}}"
  }
}
```

`number` tidak perlu dimasukkan ke `fields`; `ExportRunner` menambahkannya otomatis sebagai kolom pertama.

#### Static candidate export dengan total progress

`CANDIDATE_DATASET` adalah static dataset ringan untuk `candidate.candidate`. Ia hanya membaca kandidat aktif pada `tenant_pool_keys`, mendukung kolom `candidate_id`/`id`, `name`, serta `email`/`email_address`, dan menghitung total sebelum file dibuat.

```json
{
  "tenant_id": "TENANT_UUID",
  "email": "requester@example.com",
  "actor_id": "ACTOR_ID",
  "orchestrator_name": "EXPORT",
  "export_config": {
    "dataset": "CANDIDATE_DATASET",
    "format": "excel_rs",
    "chunk": 500,
    "fields": [
      {"name": "Nama Lengkap", "column": "name"},
      {"name": "Email", "column": "email"}
    ],
    "template_code": "EMAIL_TEMPLATE_CODE",
    "structure_template_code": "EMAIL_STRUCTURE_CODE",
    "redirected_url": "https://app.example.com/download?key={{url}}"
  }
}
```

Tenant filter ditambahkan otomatis oleh orchestrator dan diterjemahkan dataset menjadi overlap `tenant_pool_keys`. Dataset ini sengaja menolak root filter selain `tenant_id`; gunakan dynamic export atau dataset khusus bila membutuhkan filter kandidat tambahan seperti tanggal atau status.

Writer:

- `csv` menghasilkan `.csv`;
- `excel` menghasilkan `.xlsx` melalui `excelstream`;
- `excel_rs` menghasilkan `.xlsx` melalui `rust_xlsxwriter` constant-memory dan menjadi pilihan utama untuk config baru.

`template_code`, `structure_template_code`, dan `redirected_url` wajib karena tipe Rust tidak memberi default. Tidak ada flag `send_email`; implementasi saat ini selalu publish email setelah upload berhasil.

Filter tambahan diletakkan pada root:

```json
{
  "filters": [
    {
      "field": "created_at",
      "operator": ">=",
      "value": "2026-08-01 00:00:00",
      "values": null,
      "casting": "Date"
    }
  ]
}
```

Static orchestrator selalu menambahkan tenant filter root. Dataset boleh menormalisasinya, misalnya `lsi_ats_candidate_dataset` mengubah `tenant_id` menjadi overlap terhadap `tenant_pool_keys`.

#### Cara membuat static export baru

1. Buat dataset di `src/application/datasets/export/<domain>/` dan implementasikan `Dataset`.
2. Pada `fetch_batch`, baca filter dan urutan kolom dari `LoadEnum::Select`.
3. Kembalikan `Vec<ExportRow>` dengan jumlah dan urutan field yang sama dengan config.
4. Register key di `build_dataset_registry()` pada [`src/application/dataset.rs`](src/application/dataset.rs).
5. Tambahkan module export dan satu test kecil untuk mapping/cursor.
6. Pakai key baru pada `export_config.dataset`.

Kontrak minimum:

```rust
#[async_trait::async_trait]
impl Dataset for EmployeeExportDataset {
    async fn get_min_cursor(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<String> {
        // fetch_batch memakai `id > cursor`, jadi return cursor sebelum row pertama.
        Ok(uuid::Uuid::nil().to_string())
    }

    async fn fetch_batch(
        &self,
        strategy: &LoadEnum,
        cursor: Option<String>,
        limit: i64,
    ) -> anyhow::Result<(Vec<ExportRow>, Option<String>)> {
        let (filters, columns) = match strategy {
            LoadEnum::Select(filters, columns) => {
                (filters.clone(), columns.clone().unwrap_or_default())
            }
            _ => anyhow::bail!("employee export requires Select"),
        };

        // Fetch source dengan filter, cursor eksklusif, ORDER BY cursor, LIMIT.
        // Mapping paling sederhana:
        // ExportRow::validate_json(source_row, &columns)
        todo!()
    }
}
```

Cursor static adalah eksklusif. Jika query memakai `id > cursor`, jangan mengembalikan ID minimum sebagai cursor awal karena row pertama akan terlewat. Gunakan nilai sentinel sebelum data pertama, seperti UUID nil, atau ubah kontrak dataset secara konsisten.

`ExporterColumn.fetcher` dan `is_cache` bersifat dataset-specific. Hook `Dataset::configure_export()` tersedia tetapi belum dipanggil `ExportOrchestrator`; jangan menggantungkan static export baru pada hook tersebut tanpa menyambungkannya terlebih dahulu.

### Dynamic export

Dynamic export memakai satu dataset generic, `DYNAMIC_EXPORT_DATASET`. Source, relation, field, transform, dan filter ditentukan oleh JSON.

Database yang diizinkan: `organization`, `ats`, `master`, `warehouse`, dan `hris`. Schema/table/column harus identifier sederhana `[A-Za-z_][A-Za-z0-9_]*`.

Contoh candidate name dan email:

```json
{
  "type": "export",
  "tenant_id": "TENANT_UUID",
  "title": "Candidate name and email",
  "email": "requester@example.com",
  "actor_id": "ACTOR_ID",
  "orchestrator_name": "DYNAMIC_EXPORT",
  "filters": null,
  "dynamic_export_config": {
    "dataset": "DYNAMIC_EXPORT_DATASET",
    "format": "excel_rs",
    "chunk": 500,
    "fields": [
      {"name": "Nama Lengkap", "column": "name"},
      {"name": "Email", "column": "email"}
    ],
    "source": {
      "database": "ats",
      "schema": "candidate",
      "table": "candidate",
      "cursor": {"column": "id", "direction": "asc"},
      "tenant_field": "tenant_pool_keys",
      "tenant_operator": "ARRAY_OVERLAPS",
      "where_fields": [
        {"field": "deleted_at", "operator": "IS NULL", "value": null, "values": null, "casting": null}
      ]
    },
    "relations": [],
    "template_code": "EMAIL_TEMPLATE_CODE",
    "structure_template_code": "EMAIL_STRUCTURE_CODE",
    "redirected_url": "https://app.example.com/download?key={{url}}"
  }
}
```

#### Dynamic field

| Field | Fungsi |
| --- | --- |
| `name` | Header output |
| `column` | JSON path, misalnya `applicant.created_at` |
| `relation` + `custom_key` | Resolusi relation EAV/custom field |
| `custom_json_key` | Ambil key dari JSON pada kolom custom |
| `transform` | `join:<separator>`, `uppercase`, `lowercase`, `date:<format>`, atau `decrypt` |
| `default` | Nilai ketika hasil kosong; default internal `-` |

`decrypt` hanya diproses untuk kolom langsung pada source utama; path bertitik seperti `relation.secret` tidak didecrypt.

#### Dynamic relation

```json
{
  "name": "vacancy",
  "source": {
    "database": "ats",
    "schema": "vacancy",
    "table": "vacancy",
    "where_fields": []
  },
  "local_key": "applicant.vacancy_id",
  "foreign_key": "id",
  "cardinality": "one",
  "depends_on": "applicant",
  "is_cache": true,
  "is_custom": false,
  "filter_source": false,
  "filter_fields": []
}
```

- `depends_on` mengatur urutan relation bertingkat.
- `cardinality: one` memilih row pertama; nilai lain diperlakukan sebagai many.
- `order_by` menentukan row pertama saat cardinality one.
- `filter_source: true` membuat correlated `EXISTS`; relation ini harus berada pada database yang sama dengan source utama.
- `filter_fields` merutekan filter root tertentu ke relation filter source.
- `is_cache` memakai Moka TTL 30 menit, kapasitas 10.000 entry.

Operator dynamic filter yang diizinkan:

```text
= != <> > >= < <= IS NULL IS NOT NULL IN NOT IN
ARRAY_OVERLAPS / &&
JSONB_CONTAINS / @>
```

Cursor dynamic dibandingkan sebagai text. Gunakan UUID/string yang urutan leksikografisnya stabil. Cursor angka berbentuk text dapat menghasilkan urutan `1, 10, 2`.

## Flow IMPORT

### End-to-end

```mermaid
flowchart LR
    Producer -->|stream:import| Handler[ImportHandler]
    Handler --> IO[ImportOrchestrator]
    IO --> Log[(loggings)]
    IO --> S3[Facilitator get_download_url]
    S3 --> Excel[Sheet pertama XLSX]
    Excel --> Dataset[ImporterDatasetV1]
    Dataset --> Fetcher[DataFetcher registry/cache]
    Dataset --> Pipeline[Import pipeline]
    Pipeline --> Loader[Temporary loader]
    Loader --> Warehouse[(Warehouse temporary + entity states)]
    IO -->|auto_sync| Sync[stream:sync]
```

Urutan aktual:

1. Orchestrator membuat logging `IMPORT/PROCESSING` dan menyimpan config job.
2. `import_config.key` dikirim ke facilitator untuk memperoleh URL download.
3. Sheet pertama dibaca mulai `start_row`.
4. Row dibagi per `chunk`; dependency fetcher diprefetch per chunk.
5. Dataset memetakan cell ke main table, child table, atau EAV.
6. Pipeline menjalankan loader config secara berurutan.
7. Orchestrator menambahkan `logging_id` ke hasil mapping non-EAV; loader menulis temporary table dan upsert `import_entity_states` dalam transaksi yang sama.
8. Logging menjadi `COMPLETED` atau `FAILED`.
9. Jika `auto_sync: true`, job SYNC dipublish dengan logging ID import.

### Memilih mode dan dataset

| Config | Perilaku |
| --- | --- |
| `data_source_type: buffer` | Seluruh file diunduh ke memory; urutan kolom Excel harus sama dengan `fields` |
| nilai selain `buffer`, disarankan `stream` | File diunduh bertahap ke `/tmp`, header dicocokkan berdasarkan nama |
| `DYNAMIC_IMPORT_DATASET` | Mapper generic berdasarkan config; tanpa validator vacancy-aware |
| `LSI_IMPORT_VACANCY_V1` | Mapper config + validasi + scope tenant/group + enrichment vacancy |

Untuk import vacancy production, gunakan `LSI_IMPORT_VACANCY_V1`. Dataset ini:

- mengisi `tenant_id` dan `group_id` dari config;
- mendukung validator `REQUIRED` dan `UNIQUE`;
- mengubah fetcher miss menjadi validation error;
- membentuk filterable attributes, keywords, permission keys;
- dapat memperluas stage/form template menjadi child rows;
- membuat publish-on careersite dari status.

`DYNAMIC_IMPORT_DATASET` cocok untuk staging generic sederhana. Fetcher miss dicatat oleh mapper, tetapi tidak digabung menjadi validation state seperti pada `LSI_IMPORT_VACANCY_V1`.

### Struktur `import_config`

| Field | Wajib | Arti |
| --- | --- | --- |
| `fields` | Ya | Mapping satu field per kolom Excel |
| `chunk` | Ya | Row per batch; gunakan nilai positif |
| `tenant_id`, `group_id` | Ya | Scope data import |
| `dataset` | Ya | Key `ImporterDatasetV1` |
| `auto_sync` | Ya | Publish SYNC setelah import sukses |
| `key` | Ya | Object key file pada facilitator |
| `data_source_type` | Ya | `stream` atau `buffer` |
| `pipeline` | Tidak | Default `CONFIG_IMPORT_PIPELINE` |
| `loader` | Tidak | Legacy single loader |
| `loaders` | Tidak | Daftar loader berurutan; menang atas `loader` jika tidak kosong |
| `master_table` | Ya | Qualified temporary table utama |
| `primary_column` | Ya | Primary key yang dibuat mapper, umumnya `id` |
| `sequential_table` | Tidak | Child/EAV table dan foreign key |
| `fetcher_caching` | Tidak | Fetcher yang boleh menggunakan cache |
| `start_row` | Tidak | Baris Excel 1-based; default 2 |
| `main_filterable` | Tidak | Mapping `filterable_attribute`, khusus vacancy-aware |
| `main_searchable` | Tidak | Path pembentuk `keywords`, khusus vacancy-aware |
| `permission_key` | Tidak | Nilai `permission_keys`, khusus vacancy-aware |
| `backfield_config` | Tidak | Event tambahan setelah stream import dan auto-sync |

### Struktur field import

```json
{
  "header": "Job Position*",
  "column": "warehouse.ats_vacancy_temporary:job_position_id|warehouse.ats_vacancy_temporary:job_position_meta",
  "fetcher": "JOB_POSITION",
  "fetcher_field_key": "position_name",
  "fetcher_key": "id",
  "casting": "Varchar",
  "is_cache": true,
  "type_data": "UUID",
  "is_custom": false,
  "validation_rules": [
    {"validator": "REQUIRED", "options": {}}
  ],
  "template": {
    "value": null,
    "value_type": "select",
    "filter": [],
    "is_cache": true
  }
}
```

Aturan mapping:

- Sintaks target adalah `table:column`.
- Satu cell dapat menulis ke beberapa target dengan pemisah `|`.
- Kolom berakhiran `_meta` menerima object hasil fetcher; kolom lain menerima `fetcher_key`.
- Input berisi koma dipecah menjadi beberapa token.
- Child biasa ditentukan oleh `sequential_table[].is_custom: false`.
- EAV ditentukan oleh `sequential_table[].is_custom: true`; `LSI_IMPORT_VACANCY_V1` juga menyelaraskannya dari `field.is_custom`.
- `fetcher_field_key` adalah kolom lookup dan default ke `id`.
- `fetcher_key` adalah nilai hasil dan default ke `id`.
- `fetcher_field_filter` ada pada tipe config tetapi belum digunakan mapper aktif.
- `template` dipakai generator template, bukan mapping import utama.

Nilai `type_data` yang diterima: `TEXT`, `UUID`, `DATE`, `NUMBER`, dan `Boolean`.

### Contoh config import + auto-sync

```json
{
  "type": "import",
  "tenant_id": "TENANT_UUID",
  "title": "Vacancy import",
  "email": "requester@example.com",
  "actor_id": "ACTOR_ID",
  "orchestrator_name": "IMPORT",
  "import_config": {
    "dataset": "LSI_IMPORT_VACANCY_V1",
    "key": "OBJECT_KEY_TEMPLATE_XLSX",
    "data_source_type": "stream",
    "chunk": 250,
    "tenant_id": "TENANT_UUID",
    "group_id": "GROUP_UUID",
    "auto_sync": true,
    "pipeline": "CONFIG_IMPORT_PIPELINE",
    "loaders": ["WAREHOUSE_DYNAMIC_TEMPORARY_LOADER"],
    "master_table": "warehouse.ats_vacancy_temporary",
    "primary_column": "id",
    "start_row": 2,
    "fetcher_caching": ["JOB_POSITION"],
    "sequential_table": [],
    "fields": [
      {
        "header": "Vacancy Name*",
        "column": "warehouse.ats_vacancy_temporary:name",
        "type_data": "TEXT",
        "is_custom": false,
        "validation_rules": [{"validator": "REQUIRED", "options": {}}],
        "template": {"value": "Backend Engineer", "value_type": "text"}
      },
      {
        "header": "Job Position*",
        "column": "warehouse.ats_vacancy_temporary:job_position_id|warehouse.ats_vacancy_temporary:job_position_meta",
        "fetcher": "JOB_POSITION",
        "fetcher_field_key": "position_name",
        "fetcher_key": "id",
        "casting": "Varchar",
        "is_cache": true,
        "type_data": "UUID",
        "is_custom": false,
        "validation_rules": [{"validator": "REQUIRED", "options": {}}],
        "template": {"value": null, "value_type": "select", "is_cache": true}
      }
    ]
  },
  "sync_config": {
    "sync_aggregate": "LSI_VACANCY_SYNC",
    "logging_id": null,
    "chunk": 100,
    "source": null,
    "dataset": null,
    "loaders": []
  }
}
```

Pada auto-sync, worker mengganti `sync_config.logging_id` dengan ID logging import yang baru dibuat.

### Menyiapkan migration warehouse untuk import

Import tidak membuat table otomatis. Sebelum config memakai `master_table` atau `sequential_table`, seluruh table tersebut harus dibuat melalui migration pada database `WAREHOUSE_DATABASE_URL`.

```mermaid
flowchart LR
    Excel[XLSX] --> Dataset[Import dataset]
    Dataset --> Loader[WAREHOUSE_DYNAMIC_TEMPORARY_LOADER]
    Loader --> Stage[(warehouse.vacancy)]
    Stage --> Source[WAREHOUSE_VACANCY_SOURCE]
    Source --> SyncDataset[Sync dataset]
    SyncDataset --> AtsLoader[ATS bulk loader]
    AtsLoader --> OLTP[(ATS vacancy.vacancy)]
```

Contoh: bila target akhirnya `vacancy.vacancy` pada database ATS, buat staging `warehouse.vacancy` pada database warehouse. Implementasi vacancy yang sudah ada memakai nama `warehouse.ats_vacancy_temporary`; prinsipnya sama.

Yang dimaksud mirror OLTP:

- default-nya salin seluruh business column target OLTP 1:1, bukan hanya kolom yang kebetulan ada pada template Excel pertama;
- nama kolom dan tipe PostgreSQL harus kompatibel dengan target OLTP;
- `id`/`primary_column` wajib UUID karena `import_entity_states` mem-parsing entity ID sebagai UUID;
- tambahkan `logging_id VARCHAR NOT NULL`; orchestrator mengisinya dan config sync saat ini memfilter sebagai `Varchar`;
- beri index pada `logging_id` karena semua detail import, sync, dan rollback dipisahkan dengan nilai ini;
- kolom yang tidak selalu diisi Excel harus nullable atau memiliki default;
- enum OLTP umumnya disimpan sebagai `TEXT` di staging, lalu dikonversi kembali oleh sync dataset/loader;
- jangan menyalin foreign key, trigger, atau business constraint OLTP secara buta. Staging perlu menerima row invalid agar error dapat dicatat sebelum sync;
- table child juga membutuhkan `id UUID`, foreign key ke entity staging, dan `logging_id` jika akan dibaca atau di-rollback per import.

Pengecualian mirror hanya untuk constraint/trigger OLTP dan kolom operasional staging. Setiap perubahan schema OLTP yang memengaruhi import atau sync harus disertai migration warehouse pada release yang sama.

Ambil struktur aktual dari database sumber; jangan menebak tipe dari contoh JSON atau struct Rust:

```sql
SELECT
    column_name,
    data_type,
    udt_name,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'vacancy'
  AND table_name = 'vacancy'
ORDER BY ordinal_position;
```

Karena ATS dan warehouse adalah database berbeda, hasil tersebut diterjemahkan menjadi migration warehouse. Buat pasangan file `up` dan `down`:

```text
migrations/<timestamp>_create_warehouse_vacancy.up.sql
migrations/<timestamp>_create_warehouse_vacancy.down.sql
```

Contoh `up` minimal—tambahkan seluruh kolom yang dipakai mapping dan sync sesuai DDL `vacancy.vacancy`:

```sql
CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.vacancy (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITHOUT TIME ZONE,
    tenant_id UUID NOT NULL,
    group_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    status TEXT NOT NULL,
    job_position_id UUID,
    job_position_meta JSONB,
    permission_keys TEXT[],
    keywords TEXT[],
    filterable_attribute JSONB,
    logging_id VARCHAR NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_warehouse_vacancy_logging_id
    ON warehouse.vacancy (logging_id);
```

Contoh `down`:

```sql
DROP TABLE IF EXISTS warehouse.vacancy;
```

Jangan drop schema `warehouse` pada migration `down` karena dipakai table lain. Migration dijalankan otomatis oleh bootstrap hanya pada database warehouse. Setelah startup, verifikasi struktur:

```sql
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'warehouse'
  AND table_name = 'vacancy'
ORDER BY ordinal_position;
```

Config import kemudian harus memakai nama qualified yang sama:

```json
{
  "master_table": "warehouse.vacancy",
  "primary_column": "id",
  "loaders": ["WAREHOUSE_DYNAMIC_TEMPORARY_LOADER"],
  "fields": [
    {
      "header": "Vacancy Name*",
      "column": "warehouse.vacancy:name",
      "type_data": "TEXT",
      "is_custom": false,
      "validation_rules": [{"validator": "REQUIRED", "options": {}}]
    }
  ]
}
```

Nama `master_table` harus identik pada migration, config field (`warehouse.vacancy:<column>`), source sync, dan entity-state query. Satu perbedaan nama akan membuat loader tidak menemukan hasil mapping atau sync tidak menemukan entity.

#### Checklist perubahan schema

Setiap penambahan/rename kolom harus diperiksa di lima tempat:

1. migration warehouse `up/down`;
2. `import_config.fields[].column`;
3. model dan `columns()` pada source warehouse;
4. sync dataset yang membentuk payload target;
5. bulk loader ATS beserta query insert/upsert.

### Membuat fetcher untuk import

Fetcher menerjemahkan nilai Excel menjadi ID/meta sebelum row ditulis. Ia mengimplementasikan `DataFetcherJson` dan dipilih dari `fields[].fetcher`. Contoh aktif yang sederhana adalah `IngestionJobLevelSource` pada [`src/infrastructure/source/oltp/organization/core/ingestion_job_level.rs`](src/infrastructure/source/oltp/organization/core/ingestion_job_level.rs), dengan registry key `JOB_LEVEL`.

Pola implementasinya:

```rust
#[derive(sqlx::FromRow, serde::Serialize)]
struct JobLevelLookup {
    id: uuid::Uuid,
    name: String,
}

pub struct JobLevelFetcher {
    pub pool: std::sync::Arc<OrganizationClient>,
}

#[async_trait::async_trait]
impl DataFetcherJson for JobLevelFetcher {
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let mut query = sqlx::QueryBuilder::new(
            "SELECT id, name FROM core.job_levels",
        );

        fetch_data_json::<JobLevelLookup>(
            &mut query,
            filters,
            &self.pool.pool,
            None,
        )
        .await
    }
}
```

Gunakan `fetch_data_json` agar filter, casting, single value, dan multi-value mengikuti perilaku fetcher lain. Model `FromRow` harus sama dengan kolom/alias pada `SELECT`, dan seluruh field output yang dirujuk oleh `fetcher_key` atau template harus diserialisasi.

Register di [`src/application/fetch_source_regitry.rs`](src/application/fetch_source_regitry.rs) dengan key uppercase:

```rust
registry
    .register(
        "JOB_LEVEL",
        Arc::new(IngestionJobLevelSource {
            pool: manager.organization.clone(),
        }),
    )
    .expect("register JOB_LEVEL fetcher");
```

Pakai key yang sama pada config:

```json
{
  "header": "Job Level*",
  "column": "warehouse.vacancy:job_level_id|warehouse.vacancy:job_level_meta",
  "fetcher": "JOB_LEVEL",
  "fetcher_field_key": "name",
  "fetcher_key": "id",
  "casting": "Varchar",
  "is_cache": true,
  "type_data": "UUID",
  "is_custom": false
}
```

Pada contoh tersebut, Excel berisi nama job level. Mapper mencari `name`, menulis `id` ke `job_level_id`, dan menulis seluruh object hasil fetcher ke kolom berakhiran `_meta`.

Checklist fetcher:

1. query hanya memilih kolom lookup dan output yang diperlukan;
2. `fetcher_field_key` memiliki index pada database sumber bila datanya besar;
3. casting config sama dengan tipe kolom lookup;
4. key registry uppercase dan unik;
5. tambahkan key ke `fetcher_caching` hanya untuk data yang aman disimpan lintas chunk selama TTL cache;
6. uji found, not found, multi-value, dan tenant/group scope bila source bersifat tenant-specific.

### Membuat source warehouse untuk sync

Source berbeda dari fetcher. Import phase tidak membaca `SourceRegistry`; ia langsung menulis `master_table` melalui loader. Source dibutuhkan ketika SYNC membaca kembali staging berdasarkan `logging_id` dan cursor.

Contoh aktif adalah [`src/infrastructure/source/warehouse/temporary/ats/ats_vacancy_temporary.rs`](src/infrastructure/source/warehouse/temporary/ats/ats_vacancy_temporary.rs). `AtsVacancyTemporarySource`:

- memiliki model `FromRow + Serialize` yang mengikuti table warehouse;
- mendefinisikan table dan daftar `SELECT` melalui `BuildTableTrait`;
- mengimplementasikan `get_min_id()` dan `extract()` dari `SourceIncrementalExtract`;
- menggunakan `ORDER BY id ASC`, cursor `id > cursor`, filter `logging_id`, dan `LIMIT`;
- juga mengimplementasikan `DataFetcherJson`, sehingga struct yang sama dapat diregister sebagai source dan fetcher bila diperlukan.

Untuk membuat `WarehouseVacancySource`:

1. Tambahkan variant `WarehouseTemporaryTable::Vacancy` yang mengembalikan `warehouse.vacancy` pada [`src/infrastructure/loader/warehouse/types.rs`](src/infrastructure/loader/warehouse/types.rs).
2. Buat model dengan tipe yang cocok dengan migration.
3. Buat struct source berisi `Arc<WarehouseClient>`.
4. Implementasikan `BuildTableTrait<WarehouseTemporaryTable>` dan daftar kolom/alias yang diperlukan sync dataset.
5. Implementasikan `get_min_id()` serta `extract()` seperti `AtsVacancyTemporarySource`.
6. Pastikan semua filter dibind sebagai parameter; jangan menerima nama kolom/operator mentah dari request publik.
7. Export module melalui seluruh `mod.rs` pada path tersebut.

Kontrak intinya:

```rust
impl BuildTableTrait<WarehouseTemporaryTable> for WarehouseVacancySource {
    fn table(&self) -> WarehouseTemporaryTable {
        WarehouseTemporaryTable::Vacancy
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "tenant_id",
            "group_id",
            "name",
            "status",
            "job_position_id",
            "job_position_meta",
            "logging_id",
        ]
    }
}
```

Register source pada `build_source_registry()` dengan key baru uppercase:

```rust
registry
    .register(
        "WAREHOUSE_VACANCY_SOURCE",
        Arc::new(WarehouseVacancySource {
            pool: warehouse_db.clone(),
        }),
    )
    .expect("register WAREHOUSE_VACANCY_SOURCE");
```

Jika source yang sama juga diperlukan sebagai fetcher, register instance dengan key berbeda pada `build_data_fetcher_json_registry()`:

```rust
registry
    .register(
        "WAREHOUSE_VACANCY_FETCHER",
        Arc::new(WarehouseVacancySource {
            pool: manager.warehouse.clone(),
        }),
    )
    .expect("register WAREHOUSE_VACANCY_FETCHER");
```

Contoh existing untuk pola ganda ini adalah `AtsVacancyTemporarySource`: key source legacy-nya `warehouse_ats_vacancy_temporary`, sedangkan key fetcher-nya `WAREHOUSE_ATS_VACANCY_TEMPORARY`.

Setelah source, sync dataset, dan bulk loader baru terdaftar, config sync dapat menunjuk key uppercase tersebut:

```json
{
  "sync_aggregate": "CONFIG_SYNC_PIPELINE",
  "logging_id": "IMPORT_LOGGING_UUID",
  "chunk": 100,
  "source": "WAREHOUSE_VACANCY_SOURCE",
  "dataset": "VACANCY_SYNC_DATASET",
  "loaders": ["ATS_VACANCY"]
}
```

`VACANCY_SYNC_DATASET` pada contoh adalah key komponen baru yang harus dibuat dan diregister; jangan menaruh key tersebut pada config sebelum registry tersedia.

### Aturan nama registry baru

Semua key config-facing baru memakai uppercase `UPPER_SNAKE_CASE`:

| Komponen | Pola | Contoh |
| --- | --- | --- |
| Fetcher | `<SYSTEM>_<ENTITY>_FETCHER` | `WAREHOUSE_VACANCY_FETCHER` |
| Source | `<SYSTEM>_<ENTITY>_SOURCE` | `WAREHOUSE_VACANCY_SOURCE` |
| Import dataset | `<ENTITY>_IMPORT_DATASET` | `VACANCY_IMPORT_DATASET` |
| Import loader | `<SYSTEM>_<ENTITY>_LOADER` | `WAREHOUSE_VACANCY_LOADER` |
| Sync dataset | `<ENTITY>_SYNC_DATASET` | `VACANCY_SYNC_DATASET` |
| Bulk loader | `<TARGET>_<ENTITY>` | `ATS_VACANCY` |
| Pipeline/orchestrator | `<ENTITY>_<FLOW>_PIPELINE` | `VACANCY_IMPORT_PIPELINE` |

Jangan mengubah case saat membaca config. Registry bersifat case-sensitive. Simpan key sebagai constant bila dipakai dari kode dan config, serta hindari satu key yang berarti dua peran berbeda.

### Memuat fungsi import dari config

Config import bekerja melalui empat registry:

```text
import_config.dataset
    -> ImporterDatasetV1: Excel row menjadi ImportTableResult
import_config.pipeline
    -> PipelineImport: menjalankan loader
import_config.loaders[] / loader
    -> ImportLoadPersistence: menulis temporary data
fields[].fetcher
    -> DataFetcherJson: lookup label menjadi ID/meta
```

Untuk menambah lookup tanpa membuat dataset baru:

1. Implementasikan `DataFetcherJson`.
2. Register key di [`src/application/fetch_source_regitry.rs`](src/application/fetch_source_regitry.rs).
3. Isi `fetcher`, `fetcher_field_key`, dan `fetcher_key` pada field config.
4. Tambahkan key yang sama ke `fetcher_caching` bila lookup boleh di-cache.

Contoh menggunakan fetcher HRIS `EMPLOYEE`:

```json
{
  "header": "PIC Email",
  "column": "warehouse.ats_vacancy_pic:employee_id|warehouse.ats_vacancy_pic:employee_meta",
  "fetcher": "EMPLOYEE",
  "fetcher_field_key": "email",
  "fetcher_key": "id",
  "casting": "Varchar",
  "type_data": "UUID",
  "is_custom": false
}
```

Pastikan `warehouse.ats_vacancy_pic` ada pada `sequential_table` dengan `fk_column: vacancy_id`.

Untuk target temporary baru:

1. Implementasikan `ImporterDatasetV1` hanya jika sintaks generic `table:column` tidak cukup.
2. Register dataset di `build_importer_dataset_registry_v1()`.
3. Implementasikan `ImportLoadPersitence` bila loader generic tidak cocok.
4. Register loader di `build_import_loader_registry()`.
5. Pakai key registry pada config.

Nama table dan column pada dynamic import dirangkai menjadi SQL oleh loader. Perlakukan tenant config sebagai konfigurasi admin tepercaya; jangan menerima `master_table`, `sequential_table`, atau `fields[].column` langsung dari user publik.

### Membuat template XLSX dari import config

Kirim config yang sama ke controller `/generated-template`. Controller membaca `import_config.fields`, membuat sheet `main_import`, lalu membuat dropdown pada sheet `master_template` dari `template.options` atau fetcher.

```bash
curl -X POST http://127.0.0.1:8080/generated-template \
  -H 'content-type: application/json' \
  --data-binary @import-job.json \
  -o template_import.xlsx
```

`template.value_type` menerima `text`, `number`, `select`, dan `multipleSelect`. Untuk pilihan statis:

```json
{
  "header": "Status",
  "column": "warehouse.ats_vacancy_temporary:status",
  "is_custom": false,
  "template": {
    "value": "DRAFT",
    "value_type": "select",
    "options": ["DRAFT", "PUBLISHED", "CLOSED"]
  }
}
```

## Flow SYNC

### End-to-end

```mermaid
flowchart LR
    Import -->|auto_sync / manual| Redis[stream:sync]
    Redis --> Handler[SyncHandler]
    Handler --> SO[SyncOrchestrator]
    SO --> Aggregator[Sync Aggregator]
    Aggregator --> Source[(Warehouse temporary)]
    Source --> Dataset[Sync Dataset]
    Dataset --> Loader[ATS Bulk Loaders]
    Loader --> ATS[(ATS transaction per batch)]
    SO --> Log[(Update import logging)]
```

SYNC tidak membuat logging baru. Ia memperbarui logging import yang ditunjuk `sync_config.logging_id`.

Status normal:

```text
COMPLETED -> SYNC_PROGRESS -> SYNC_SUCCESS
                             -> PARTIAL_SYNC
                             -> SYNC_FAILED
```

### Legacy vacancy sync yang direkomendasikan

`LSI_VACANCY_SYNC` menggunakan source, dataset, dan loader yang sudah ditetapkan di kode. Field `source`, `dataset`, dan `loaders` pada config diabaikan.

```json
{
  "type": "sync",
  "tenant_id": "TENANT_UUID",
  "email": "requester@example.com",
  "actor_id": "ACTOR_ID",
  "orchestrator_name": "SYNC",
  "sync_config": {
    "sync_aggregate": "LSI_VACANCY_SYNC",
    "logging_id": "IMPORT_LOGGING_UUID",
    "chunk": 100
  }
}
```

Aggregator ini:

- hanya menyinkronkan entity dengan validation state `VALID`;
- menandai entity invalid sebagai `SYNC_FAILED` tanpa mengirimkannya ke ATS;
- menjalankan enam loader vacancy dalam satu transaksi per batch;
- menandai entity valid `SYNC` setelah sukses;
- menghasilkan `PARTIAL_SYNC` jika ada campuran entity valid dan invalid.

### Config sync pipeline

`CONFIG_SYNC_PIPELINE` memilih source, dataset, dan urutan loader dari config.

```json
{
  "type": "sync",
  "tenant_id": "TENANT_UUID",
  "email": "requester@example.com",
  "actor_id": "ACTOR_ID",
  "orchestrator_name": "SYNC",
  "sync_config": {
    "sync_aggregate": "CONFIG_SYNC_PIPELINE",
    "logging_id": "IMPORT_LOGGING_UUID",
    "chunk": 100,
    "source": "warehouse_ats_vacancy_temporary",
    "dataset": "LSI_VACANCY_SYNC_DATASET",
    "loaders": [
      "ATS_VACANCY",
      "ATS_VACANCY_CUSTOM_FIELD",
      "ATS_VACANCY_PIC",
      "ATS_VACANCY_STAGE",
      "ATS_VACANCY_REQUIREMENT",
      "ATS_VACANCY_PUBLISH_ON"
    ]
  }
}
```

Semua loader dalam satu batch memakai satu transaksi ATS. Urutan dipertahankan. `chunk <= 0` dinormalisasi menjadi 1 oleh config pipeline.

Config pipeline saat ini tidak menjalankan lifecycle entity-state selengkap `LSI_VACANCY_SYNC`; untuk vacancy production, pilih aggregator legacy sampai lifecycle tersebut disamakan.

### Menambah sync baru

1. Register source yang mengimplementasikan `SourceIncrementalExtract`.
2. Implementasikan `SyncDataset` untuk membentuk `SycnTableResult`.
3. Register dataset di `build_sync_dataset_registry()`.
4. Implementasikan satu `BulkLoaderTrait` per target table dan register di bulk loader registry.
5. Gunakan `CONFIG_SYNC_PIPELINE` jika komposisi config sudah cukup; buat `SyncAggregator` baru bila perlu validasi/lifecycle khusus.
6. Uji idempotensi, conflict target, transaksi, dan status entity per batch.

## Flow INGESTION

### End-to-end

```mermaid
flowchart LR
    Redis[stream:ingestion] --> Handler[IngestionHandler]
    Handler --> IO[IngestionOrchestrator]
    IO --> Loader[Loader Registry]
    Loader --> Watermark[Max source_updated_at]
    IO --> Pipeline[ingestion pipeline]
    Pipeline --> Source[Source Registry]
    Source --> Delta[Rows updated after watermark]
    Delta --> Loader
    Loader --> OLAP[(Warehouse OLAP)]
```

Config:

```json
{
  "type": "ingestion",
  "tenant_id": "GLOBAL",
  "email": "system@worker",
  "actor_id": "system",
  "orchestrator_name": "ingestion",
  "ingestion_config": {
    "pipeline": "ingestion",
    "source": "oltp_ats_vacancy",
    "loader": "OLAP_ATS_VACANCY_DIMENSION",
    "dataset": null,
    "chunk": 500
  }
}
```

Urutan aktual:

1. Loader membaca watermark maksimum `source_updated_at` dari target.
2. Source menghitung seluruh row yang lebih baru.
3. Pipeline mengambil seluruh delta dalam satu request.
4. Loader menulis seluruh delta.
5. Setelah vacancy ingestion, pipeline mempublish aggregate vacancy ke `stream:aggregate`.

`ingestion_config.dataset` belum digunakan. `chunk` wajib positif tetapi implementasi saat ini memuat seluruh delta sekaligus dengan limit `total_data`; nilai `chunk` belum membagi proses menjadi batch.

### Pasangan ingestion yang aktif

Hanya source berikut yang mengimplementasikan `count_updated_after` dan `extract_updated_after` serta memiliki loader watermark-compatible:

| Source | Loader |
| --- | --- |
| `oltp_ats_vacancy` | `OLAP_ATS_VACANCY_DIMENSION` |
| `master_items` | `OLAP_MASTER_ITEM_DIMENSION` |
| `master_geo` | `OLAP_GEO_DIMENSION` |

Source registry lain dapat dipakai oleh export/sync tetapi akan mengembalikan error default jika dipakai untuk ingestion updated-after.

INGESTION belum membuat record pada tabel `loggings`; observasi flow ini mengandalkan application logs dan hasil target OLAP.

### Menambah ingestion baru

1. Implementasikan `count_updated_after` dan `extract_updated_after` pada source.
2. Pastikan query menggunakan `ORDER BY updated_at, id` yang deterministik.
3. Implementasikan loader dengan `load()` dan `fetch_max_source_updated_at()`.
4. Register source dan loader.
5. Tambahkan pasangan key ke dokumentasi dan test incremental watermark.
6. Jika volume delta dapat besar, perbaiki orchestrator menjadi cursor `(updated_at, id)` sebelum digunakan production.

## Flow ROLLBACK

### End-to-end

```mermaid
flowchart LR
    API[POST /rollback] --> Redis[stream:rollback]
    Redis --> Handler[RollbackHandler]
    Handler --> RO[RollbackOrchestrator]
    RO --> Log[(Logging ROLLBACK)]
    RO --> Pipeline[CONFIG_ROLLBACK_PIPELINE]
    Pipeline --> Loaders[Fixed-table rollback loaders]
    Loaders --> ATS[(One ATS transaction)]
```

Rollback menghapus row ATS dengan `logging_id` yang sama. Target table tidak diambil dari payload; setiap key loader sudah terikat ke table tetap di kode.

```json
{
  "type": "rollback",
  "tenant_id": "TENANT_UUID",
  "title": "Rollback vacancy import",
  "email": "requester@example.com",
  "actor_id": "ACTOR_ID",
  "orchestrator_name": "ROLLBACK",
  "rollback_config": {
    "pipeline": "CONFIG_ROLLBACK_PIPELINE",
    "logging_id": "IMPORT_LOGGING_UUID",
    "loaders": [
      "ATS_VACANCY_REQUIREMENT_ROLLBACK",
      "ATS_VACANCY_STAGE_ROLLBACK",
      "ATS_VACANCY_PUBLISH_ON_ROLLBACK",
      "ATS_VACANCY_PIC_ROLLBACK",
      "ATS_VACANCY_CUSTOM_FIELD_ROLLBACK",
      "ATS_VACANCY_ROLLBACK"
    ]
  }
}
```

Urutkan child sebelum parent untuk memenuhi foreign key. Seluruh delete berjalan dalam satu transaksi; satu kegagalan membatalkan semuanya.

Rollback membuat logging baru bertipe `ROLLBACK`. Logging import/sync target tidak diubah dan temporary warehouse tidak dibersihkan.

## HTTP controller

Router berada pada `src/bin/http.rs` dan `src/bin/http_worker.rs`. Belum ada authentication/authorization middleware di service ini; deployment harus mengandalkan gateway/internal network dan tidak boleh mengekspos controller administratif secara publik.

### Daftar endpoint

| Method | Path | Fungsi |
| --- | --- | --- |
| GET | `/healthz` | Liveness HTTP process |
| POST | `/detail-file` | Resolve object key lalu redirect ke pre-signed URL |
| POST | `/loggings?page=1&limit=10` | List logging tenant |
| GET | `/loggings/:logging_id/import-detail` | Detail row dan validation/sync state import |
| POST | `/generated-template` | Generate XLSX template import |
| POST | `/test-import` | Publish payload ke stream berdasarkan `type` |
| POST | `/rollback` | Validasi request minimal dan enqueue rollback |

### GET `/healthz`

```json
{
  "status": "ok",
  "date": "2026-08-25T00:00:00+00:00"
}
```

Endpoint hanya membuktikan HTTP process hidup, bukan health Redis/database/gRPC.

### POST `/detail-file`

Request:

```json
{"keys": "FACILITATOR_OBJECT_KEY"}
```

Response sukses adalah HTTP redirect ke pre-signed URL. Gunakan `curl -L` untuk mengikuti redirect.

```bash
curl -L -X POST http://127.0.0.1:8080/detail-file \
  -H 'content-type: application/json' \
  -d '{"keys":"FACILITATOR_OBJECT_KEY"}' \
  -o result.xlsx
```

Endpoint belum memverifikasi kepemilikan key terhadap tenant.

### POST `/loggings`

```bash
curl -X POST 'http://127.0.0.1:8080/loggings?page=1&limit=20' \
  -H 'content-type: application/json' \
  -H 'X-TIMEZONE: Asia/Makassar' \
  -d '{
    "_auth_context": {"tenantId": "TENANT_ID"},
    "filter": {"loggingType": "IMPORT", "status": "COMPLETED"},
    "sort": {"createdAt": "DESC"}
  }'
```

`page` default 1, `limit` default 10 dan maksimum 100. `X-TIMEZONE` harus nama IANA; fallback ke environment `TIMEZONE`, lalu UTC.

Filter mendukung `id`, `actorId`, `email`, `tenantId`, `loggingType`, `status`, dan `streamId`. `loggingType` pada controller saat ini hanya menerima `EXPORT`, `IMPORT`, `ROLLBACK`, dan `AGGREGATE`.

Tenant filter dari body ditimpa oleh `_auth_context.tenantId`. Pastikan gateway selalu mengisi auth context; jika kosong, service tidak menambahkan tenant restriction.

### GET `/loggings/:logging_id/import-detail`

Endpoint hanya menerima logging bertipe IMPORT. Response berisi:

- `total_processed`, `total_success`, `total_failed`;
- `headers` dari `import_config.fields` plus status/error metadata;
- `content` dari main temporary row dan sequential rows.

Endpoint membaca nama table dari config logging dengan validasi identifier, tetapi belum memeriksa tenant/auth context.

### POST `/generated-template`

Jika body memiliki `import_config`, controller langsung menurunkan header dan dropdown dari config tersebut. Response adalah attachment `template_import.xlsx`.

```bash
curl -X POST http://127.0.0.1:8080/generated-template \
  -H 'content-type: application/json' \
  --data-binary @import-job.json \
  -o template_import.xlsx
```

Mode manual juga tersedia:

```json
{
  "tenant_id": "TENANT_ID",
  "headers": [
    {"header": "Name", "background_color": "#D9EAF7"},
    {"header": "Status"}
  ],
  "example_value": [
    {"value": "Backend Engineer", "value_type": "text"},
    {"value": "DRAFT", "value_type": "select", "options": ["DRAFT", "PUBLISHED"]}
  ],
  "payload": {"tenant_id": "TENANT_ID"}
}
```

### POST `/test-import`

Nama endpoint legacy; controller dapat mempublish `import`, `export`, `sync`, `ingestion`, `rollback`, atau `aggregate`.

```bash
curl -X POST http://127.0.0.1:8080/test-import \
  -H 'content-type: application/json' \
  --data-binary @job.json
```

Controller memilih stream dari root `type`, fallback ke `orchestrator_name`, lalu meneruskan payload tanpa validasi kontrak job. Gunakan hanya sebagai endpoint internal/testing.

### POST `/rollback`

```bash
curl -X POST http://127.0.0.1:8080/rollback \
  -H 'content-type: application/json' \
  -d '{
    "tenant_id": "TENANT_ID",
    "email": "requester@example.com",
    "actor_id": "ACTOR_ID",
    "logging_id": "IMPORT_LOGGING_UUID",
    "loaders": [
      "ATS_VACANCY_REQUIREMENT_ROLLBACK",
      "ATS_VACANCY_ROLLBACK"
    ]
  }'
```

Response sukses `202 Accepted`. `pipeline` default ke `CONFIG_ROLLBACK_PIPELINE`.

## Logging dan status

Tabel `loggings` berada di warehouse. Lookup update menerima ID logging maupun Redis stream message ID.

| Flow | Membuat logging | Catatan |
| --- | --- | --- |
| Static/dynamic EXPORT | Ya | total count/chunk hanya dihitung dynamic export |
| IMPORT | Ya | config lengkap disimpan untuk detail import |
| SYNC | Tidak | memperbarui logging import |
| INGESTION | Tidak | observasi melalui tracing/target data |
| ROLLBACK | Ya | target import ID disimpan pada `additional_info` |

Status yang digunakan:

```text
PENDING
PROCESSING
COMPLETED
FAILED
PARTIAL_SUCCESS
SYNC_PROGRESS
SYNC_SUCCESS
PARTIAL_SYNC
SYNC_FAILED
```

Known behavior:

- IMPORT, SYNC, dan ROLLBACK mencatat failure setelah logging tersedia.
- EXPORT dapat meninggalkan status `PROCESSING` jika gagal setelah logging dibuat karena jalur error belum mengubahnya menjadi `FAILED`.
- Invalid JSON atau registry error yang terjadi sebelum logging dibuat hanya terlihat pada application log.

## Menambah kemampuan baru

### Menambah fetcher import/template

1. Implementasikan `DataFetcherJson`.
2. Batasi kolom dan filter yang dibutuhkan.
3. Register di `build_data_fetcher_json_registry()`.
4. Tambahkan test filter/model.
5. Dokumentasikan `fetcher_field_key`, `fetcher_key`, dan display field template.

### Menambah static export

1. Implementasikan `Dataset`.
2. Pastikan cursor eksklusif tidak melewatkan row pertama.
3. Mapping field mengikuti urutan `LoadEnum::Select(..., columns)`.
4. Register di `build_dataset_registry()`.
5. Tambahkan config contoh dan test batching.

### Menambah import target

1. Coba mapper `DYNAMIC_IMPORT_DATASET` dan loader generic terlebih dahulu.
2. Jika ada business enrichment/validation, wrap `DynamicDatasetV1` seperti `LSI_IMPORT_VACANCY_V1`.
3. Jika persistence berbeda, implementasikan `ImportLoadPersitence`.
4. Register dataset/loader dan gunakan key dari config.
5. Pastikan temporary table memiliki `id` UUID dan `logging_id` untuk detail/sync/rollback ownership.

### Menambah sync target

1. Implementasikan source temporary.
2. Implementasikan `SyncDataset`.
3. Implementasikan bulk loader per table.
4. Pilih config pipeline atau aggregator khusus.
5. Pastikan semua loader batch memakai transaksi dan upsert idempotent.
6. Update entity validation/sync state secara konsisten.

### Menambah ingestion pair

1. Source harus mengimplementasikan count/extract updated-after.
2. Loader harus menyimpan dan membaca source watermark.
3. Register kedua key.
4. Uji first load, no-op load, update existing row, dan equal-timestamp tie.

### Menambah rollback table

1. Tambahkan fixed mapping key ke table di `build_rollback_loader_registry()`.
2. Pastikan target memiliki `logging_id`.
3. Tempatkan child sebelum parent pada config.
4. Tambahkan test transaksi dan foreign key.

## Operasional dan troubleshooting

### Job tidak berjalan

Periksa berurutan:

1. Stream sesuai flow.
2. Entry Redis memiliki field `payload`.
3. Root field wajib dapat dideserialisasi.
4. Registry key case-sensitive dan benar.
5. Consumer group memiliki consumer aktif.

```bash
redis-cli -u "$REDIS_URL" XINFO GROUPS stream:import
redis-cli -u "$REDIS_URL" XINFO CONSUMERS stream:import import-group
redis-cli -u "$REDIS_URL" XPENDING stream:import import-group
```

Karena failure tetap di-ACK, pesan gagal biasanya tidak berada di pending list.

### Import gagal

Periksa:

- facilitator mengembalikan URL absolut dan dapat diakses worker;
- sheet pertama berisi header yang sesuai config;
- `start_row` benar;
- mode buffer mengikuti urutan field config;
- table/column config benar dan migration warehouse sudah ada;
- fetcher key, lookup column, casting, serta output key benar;
- worker dapat menulis dan menghapus file `/tmp` pada stream mode.

Gunakan:

```bash
curl http://127.0.0.1:8080/loggings/IMPORT_LOGGING_UUID/import-detail
```

### Sync gagal

Periksa:

- `logging_id` adalah ID logging import, bukan Redis message ID;
- temporary row memiliki `logging_id` yang sama;
- `import_entity_states` memiliki row valid;
- urutan loader parent/child sesuai kebutuhan target;
- enum/casting target ATS cocok;
- satu batch gagal akan rollback seluruh batch ATS.

### Export gagal

Periksa:

- dataset/writer key terdaftar;
- `chunk > 0`;
- direktori kerja dapat menulis `exports/`;
- field/path benar untuk dataset;
- source cursor deterministik;
- facilitator upload tersedia;
- ATS decryptor tersedia jika memakai `decrypt`.

Jika logging tertahan `PROCESSING`, lihat log berdasarkan Redis `msg_id`; export failure belum selalu mengubah status menjadi `FAILED`.

### Ingestion gagal atau boros memory

Pastikan source dan loader adalah pasangan aktif. Implementasi memuat seluruh delta ke memory; jika delta besar, jalankan lebih sering atau tambahkan batching `(updated_at, id)` sebelum menaikkan volume production.

### Disk dan memory

- Export file tetap berada di `exports/` setelah upload.
- Stream import memakai file sementara `/tmp/worker-exporter-import-<uuid>.xlsx` dan mencoba menghapusnya.
- Buffer import memuat seluruh XLSX ke memory.
- Export membaca file final seluruhnya ke memory sebelum gRPC upload.
- Lookup cache import memiliki TTL 30 menit dan kapasitas 5.000 entry pada orchestrator; dynamic dataset juga memiliki cache internal.

### Security boundary

- HTTP controller belum memiliki auth middleware.
- `/test-import` meneruskan config mentah ke Redis.
- Dynamic import menggunakan table/column config dalam SQL loader; hanya config admin tepercaya yang boleh masuk.
- `/detail-file` dan import-detail belum memverifikasi tenant ownership.
- Dynamic export memvalidasi identifier dan membatasi database/operator.
- Rollback memakai table tetap dari registry, bukan table mentah dari payload.

### Deployment checklist

- Environment dan secret lengkap.
- Semua lima PostgreSQL database, Redis, dan dua gRPC endpoint reachable.
- Warehouse migration sukses.
- `exports/` dan `/tmp` writable.
- Consumer `stream:email` tersedia.
- Gateway membatasi endpoint administratif dan menyuntikkan auth context tenant.
- Redis lag/pending, logging failure, disk usage, dan memory dipantau.
- Replica/concurrency dan `chunk` telah diuji terhadap kapasitas database.
- Job `PROCESSING` diperiksa sebelum rolling restart karena belum ada graceful job drain/reclaim.

## Checklist handover

Pemilik berikutnya sebaiknya dapat melakukan seluruh item ini sebelum handover dianggap selesai:

- Menjalankan `http_worker` dan memverifikasi `/healthz`.
- Publish satu static export dan satu dynamic export.
- Generate template, upload XLSX, lalu menjalankan import config.
- Membaca detail validation import.
- Menjalankan auto-sync dan memverifikasi row ATS serta status entity.
- Menjalankan rollback dan memastikan child-parent terhapus dalam satu transaksi.
- Menjalankan tiga pasangan ingestion aktif dan memverifikasi no-op run kedua.
- Menemukan job dari Redis `msg_id`, logging ID, dan tenant.
- Menangani ulang job gagal dengan publish payload baru.
- Menambah satu registry key kecil beserta test dan config contoh.

Prioritas technical debt yang paling berdampak:

1. Tambahkan retry/DLQ dan pending-message recovery sebelum menaikkan criticality job.
2. Lindungi HTTP controller dan validasi/allowlist identifier dynamic import.
3. Ubah ingestion menjadi batching `(updated_at, id)`.
4. Tandai export failure sebagai `FAILED` dan hapus file lokal setelah upload.
5. Tambahkan readiness check dependency dan graceful worker drain.
