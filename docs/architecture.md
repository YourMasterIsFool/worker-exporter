# Worker Exporter Documentation

Dokumentasi ini menggabungkan seluruh dokumentasi Markdown project ke satu file utama.

## Dynamic Export Config

Dynamic export menggabungkan source utama, relation, field resolver, dan transform menjadi satu payload. Contoh reference check memakai alur:

`reference_check_referees -> reference_checks -> candidate -> applicant_bucket -> vacancy`

Jawaban form memakai alur:

`reference_check_referees.identifier_id -> form_responses.identifier_id -> form_questions/form_response_answers`

### Struktur minimum

```json
{
  "orchestrator_name": "EXPORT",
  "dynamic_export_config": {
    "dataset": "DYNAMIC_EXPORT_DATASET",
    "chunk": 10,
    "format": "excel",
    "source": {
      "database": "ats",
      "schema": "process",
      "table": "reference_check_referees",
      "cursor": {"column": "id", "direction": "asc"},
      "tenant_field": "tenant_id",
      "tenant_operator": "=",
      "where_fields": [{"field": "deleted_at", "operator": "IS NULL"}]
    },
    "fields": [
      {"name": "Candidate ID", "column": "candidate.id"},
      {
        "name": "Company Name",
        "relation": "form_answers",
        "custom_key": "Nama Perusahaan / Organisasi",
        "custom_json_key": {
          "column": "answer_value",
          "value": "text"
        }
      },
      {
        "name": "Internal Organization",
        "relation": "form_answers",
        "custom_key": "Apakah kandidat ini turut aktif terlibat dalam organisasi atau kegiatan di dalam perusahaan? (Did the candidate actively participating in organization or activities in the company?)",
        "custom_json_key": {
          "column": "answer_value",
          "value": "selected_options",
          "options_relation": "form_question_options"
        },
        "transform": "join:; "
      }
    ]
  }
}
```

### Field

- `name`: nama kolom hasil export.
- `column`: path langsung, misalnya `candidate.email` atau `experience.job_title`.
- `relation`: relation yang digunakan field.
- `custom_key`: key EAV atau teks `form_questions.question_text`.
- `custom_json_key.column`: kolom JSON sumber, biasanya `answer_value`.
- `custom_json_key.value`: key JSON seperti `text`, `number_value`, `date_value`, atau `selected_options`.
- `custom_json_key.options_relation`: relation untuk mengubah UUID option menjadi `option_text`.
- `transform`: transform output, misalnya `join:; `.

Untuk form answer, `custom_key` dinormalisasi menjadi lowercase dan semua whitespace dihapus. Nilai config dan database tetap dianggap sama walaupun berbeda kapital, spasi, atau newline.

### Relation

- `local_key`: path key pada row saat ini.
- `foreign_key`: kolom yang dicari pada source relation.
- `depends_on`: relation yang harus selesai lebih dulu.
- `filter_source`: jika `true`, relation dipakai untuk menyaring row source utama
  dengan correlated `EXISTS`.
- `filter_fields`: nama filter payload yang diarahkan ke relation ini. Filter yang
  tidak terdaftar tetap diarahkan ke source utama.
- `cardinality: "one"`: ambil satu row.
- `cardinality: "many"`: ambil array.
- `order_by`: sorting sebelum memilih row pertama. Gunakan `created_at DESC` untuk applicant bucket terbaru.
- `is_custom`: aktifkan resolver EAV.
- `value_columns`: mapping tipe EAV ke kolom nilai.

### Routing filter source dan relation

`filters` tetap dikirim pada root payload, bukan di dalam `dynamic_export_config`.
Worker menerapkan `tenant_id` ke source utama dan ke relation `filter_source`.
Filter lain diarahkan berdasarkan `filter_fields`:

```text
tenant_id         -> source utama dan filter relation
candidate_id      -> reference_checks
created_at        -> reference_checks jika terdaftar di filter_fields
submission_status -> reference_check_referees jika tidak terdaftar di relation
```

Contoh source referee dengan parent reference check:

```json
{
  "source": {
    "database": "ats",
    "schema": "process",
    "table": "reference_check_referees",
    "tenant_field": "tenant_id",
    "cursor": {"column": "id", "direction": "asc"}
  },
  "relations": [
    {
      "name": "reference_check",
      "filter_source": true,
      "filter_fields": ["candidate_id", "created_at"],
      "source": {
        "database": "ats",
        "schema": "process",
        "table": "reference_checks",
        "where_fields": [
          {"field": "deleted_at", "operator": "IS NULL"}
        ]
      },
      "local_key": "reference_check_id",
      "foreign_key": "id",
      "cardinality": "one"
    }
  ]
}
```

Payload filter tetap berada di root:

```json
{
  "tenant_id": "TENANT_UUID",
  "filters": [
    {"field": "candidate_id", "operator": "=", "value": "CANDIDATE_UUID"},
    {"field": "created_at", "operator": ">=", "value": "2026-08-01 00:00:00"}
  ]
}
```

Dengan konfigurasi tersebut, loop tetap membaca `reference_check_referees`,
sedangkan filter parent diterapkan melalui query `EXISTS` ke `reference_checks`.
Jika satu field ada di source dan relation, masukkan field itu ke `filter_fields`
relation hanya ketika filter tersebut memang dimaksudkan untuk relation.

### Form answer dan option

`form_response_answers.form_question_id` dijoin secara internal ke `form_questions.id`. Resolver kemudian mencocokkan `custom_key` dengan `form_questions.question_text`.

Untuk `selected_options: ["OPTION_UUID"]`, gunakan relation:

```json
{
  "name": "form_question_options",
  "local_key": "form_answers.answer_value.selected_options",
  "foreign_key": "id",
  "cardinality": "many",
  "depends_on": "form_answers"
}
```

Hasil relation mengambil `option_text`, kemudian `transform: "join:; "` menggabungkan banyak pilihan menjadi satu cell.

JSON request tidak boleh memiliki newline literal di dalam string. Tulis `custom_key` dalam satu baris atau gunakan escape `\\n`.

---

## Source: ARCHICTECTURE

# Project Architecture Review

This project is a **layered worker architecture** with event-driven processing:

`Redis Stream -> Handler -> Orchestrator -> Runner -> Source/Load/Dataset -> Writer (CSV)`

For your question:

- It **supports ingestion**: yes (incremental pull from organization DB to warehouse snapshot).
- It **supports export**: yes (warehouse snapshot to CSV file).
- It **supports OLAP**: **partially** (schema direction exists, but OLAP flow is not fully wired yet).

Overall quality: **good as a scalable skeleton, not yet good as production-ready architecture**.

Language versions:

1. Indonesian: `ID_ARCHITECTUE_MD.md`
2. English: `EN_ARCHITECTURE_MD.md`

---

## Source: EN_ARCHITECTURE_MD

# Architecture Analysis (English)

## 1) Current architecture structure

The project uses a **layered architecture with registry/factory patterns**:

1. **Application layer**: handler, orchestrator, runner, registries.
2. **Core layer**: contracts/traits (`Consumer`, `PipelineTrait`, `IncrementalLoad`, `WritterTrait`, etc).
3. **Domain layer**: entities, snapshots, dataset mapping.
4. **Infrastructure layer**: Redis consumer, PostgreSQL source, warehouse upsert/query, CSV/Excel exporters.

Active runtime flow:

1. Worker consumes `stream:ingestion` from Redis.
2. Job payload is deserialized.
3. `ExportOrchestrator` runs:
   - `LoaderRunner` (ingestion source -> warehouse snapshot)
   - `ExportRunner` (warehouse dataset -> CSV writer)
4. Status and progress are stored in `loggings`.

## 2) Does this support OLAP, ingestion, or export database?

### Ingestion
**Yes, supported** for tenant flow:

- Source: organization PostgreSQL (`TenantSource`)
- Load: warehouse upsert (`tenant_snapshots`)
- Incremental cursor-based batching

### Export (database to file)
**Yes, supported**:

- Read from warehouse snapshot (`TenantPoolQuerySnapshot`)
- Map rows via domain dataset (`TenantDataset`)
- Write CSV to `exports/` (`CsvExporter`)

### OLAP
**Partially only**:

- There are OLAP-oriented schema hints (dimension migrations)
- But end-to-end OLAP wiring is not complete yet (full fact/dim pipeline, semantic/serving query path, migration quality consistency)

So today this is best described as an **ingestion + export worker architecture**, not a complete OLAP platform.

## 3) Is the complexity enough or too much?

For the currently active feature set (tenant -> snapshot -> CSV), complexity is **medium to high**:

- **Pros**: very extensible for adding new sources/datasets/writers.
- **Cons**: several abstractions are only partially used, increasing maintenance overhead.

In short: good for future growth, somewhat over-engineered for current implemented scope.

## 4) Is this architecture good or not?

**Honest answer: good foundation, not yet production-final quality.**

### What is good

1. Clear separation between layers.
2. Trait-driven contracts allow component swapping.
3. Basic observability exists via `loggings`.
4. Incremental cursor pattern is appropriate for large-batch processing.

### What is weak

1. Compile health is not clean yet (many warnings and unresolved compile issues, especially Excel exporter `Send + Sync` constraints).
2. Some modules/migrations are incomplete or inconsistent.
3. Two export design directions coexist (legacy exporter/factory path vs active orchestrator-runner path), which increases complexity.
4. Reliability controls are still minimal (retry strategy, DLQ/failure stream policy, strict idempotency policy across steps).

## 5) Priority recommendations

1. **Keep one primary export path** (current orchestrator-runner) and retire unused legacy paths.
2. **Clean compile health** (reduce warnings, resolve/isolated Excel exporter thread-safety constraints).
3. **Fix migrations** for valid forward/backward operations.
4. **Add reliability controls**: bounded retries, dead-letter handling, and clear ack policy on failures.
5. After stabilization, continue full OLAP expansion (complete fact/dim pipelines + query-serving contract).

## 6) Short verdict

This architecture is **already suitable for early-stage ingestion and export workflows**, but **not yet mature enough to be called production-ready OLAP architecture**.

---

## Source: ID_ARCHITECTUE_MD

# Analisis Arsitektur (Bahasa Indonesia)

## 1) Struktur arsitektur saat ini

Arsitektur memakai pola **clean-ish layered + registry/factory**:

1. **Application layer**: handler, orchestrator, runner, registry.
2. **Core layer**: trait/kontrak (`Consumer`, `PipelineTrait`, `IncrementalLoad`, `WritterTrait`, dll).
3. **Domain layer**: entity, snapshot, dataset mapping.
4. **Infrastructure layer**: Redis consumer, PostgreSQL source, warehouse query/upsert, CSV/Excel exporter.

Flow utama yang aktif:

1. Worker consume `stream:ingestion` dari Redis.
2. Payload job di-parse.
3. `ExportOrchestrator` menjalankan:
   - `LoaderRunner` (ingestion source -> warehouse snapshot)
   - `ExportRunner` (dataset warehouse -> CSV writer)
4. Logging status disimpan di tabel `loggings`.

## 2) Apakah arsitektur ini support OLAP, ingestion, atau export database?

### Ingestion
**Ya, sudah support** untuk skenario tenant:

- Source: organization PostgreSQL (`TenantSource`)
- Load: upsert ke warehouse (`tenant_snapshots`)
- Mode incremental dengan cursor

### Export database / export file
**Ya, sudah support**:

- Read dari warehouse (`TenantPoolQuerySnapshot`)
- Mapping ke export row (`TenantDataset`)
- Tulis ke CSV (`CsvExporter`) ke folder `exports/`

### OLAP
**Belum penuh**:

- Ada indikasi menuju OLAP (migrations `tenant_dimensions`, `tenant_information_dimensions`, `tenant_taxes_dimensions`)
- Tetapi belum ada wiring lengkap end-to-end (fact/dim pipeline, semantic layer, serving/query path, kualitas migration belum stabil)

Kesimpulan: saat ini lebih tepat disebut **ingestion + export worker**, belum platform OLAP matang.

## 3) Kompleksitas: sudah cukup atau berlebihan?

Untuk scope yang aktif (utama tenant -> snapshot -> CSV), kompleksitas saat ini cenderung **sedang ke tinggi**:

- **Kelebihan**: extensible, mudah tambah source/dataset/writer baru.
- **Kekurangan**: banyak abstraksi belum dipakai penuh (factory/registry ganda, modul placeholder), sehingga cognitive load tinggi.

Artinya: arsitektur ini cocok untuk pertumbuhan fitur, tapi untuk fitur yang sekarang masih terasa agak over-engineered.

## 4) Arsitektur ini bagus atau tidak?

**Jawaban jujur: bagus sebagai fondasi, belum bagus untuk production final.**

### Yang sudah bagus

1. Pemisahan layer cukup jelas.
2. Trait-based contracts membuat komponen mudah diganti.
3. Sudah ada observability dasar via tabel `loggings`.
4. Pola incremental cursor sudah tepat untuk batch processing.

### Yang masih lemah

1. Masih ada banyak warning dan beberapa compile issue (terutama jalur exporter Excel yang tidak memenuhi `Send + Sync`).
2. Ada modul/migration yang belum konsisten atau belum lengkap.
3. Ada dua arah desain export yang hidup bersamaan (jalur lama dan jalur orchestrator-runner baru), menambah kompleksitas maintenance.
4. Reliability pattern masih minimal (retry policy, dead-letter handling, idempotency policy lintas step belum tegas).

## 5) Rekomendasi prioritas

1. **Stabilkan satu jalur export utama** (orchestrator-runner sekarang) dan pensiunkan jalur lama yang tidak dipakai.
2. **Rapikan compile health** (hilangkan warning mayor, selesaikan issue `Send + Sync` di Excel exporter atau isolasi fitur Excel).
3. **Perbaiki migrations** agar benar-benar valid dan bisa rollback dengan aman.
4. **Tambahkan reliability controls**: retry terbatas, DLQ/failed stream, dan kebijakan ack yang jelas saat error.
5. Setelah stabil, baru lanjut perluasan OLAP (fact/dim pipeline lengkap + contract query layer).

## 6) Verdict singkat

Saat ini arsitektur ini **sudah cocok untuk ingestion + export worker skala awal**, namun **belum cukup matang untuk disebut arsitektur OLAP production-ready**.

---

## Source: PORTOFOLIO

# Worker Exporter

## Multi-Domain, Multi-Tenant Data Worker

**Worker Exporter** adalah platform worker berbasis Rust dan Tokio untuk memproses import,
export, sinkronisasi data, serta rancangan OLAP secara asynchronous.

Project ini dibangun sebagai **modular monolith dengan multi-process deployment**: satu
codebase, satu kumpulan domain dan infrastructure, tetapi workload dapat dijalankan melalui
executable yang berbeda.

> Catatan status: IMPORT, EXPORT, SYNC, Redis Streams, HTTP API, registry, dan chunk processing
> sudah ada di project. `worker_olap`, export snapshot projector, serta pipeline aggregate
> daily/weekly/monthly/yearly adalah target pengembangan arsitektur berikutnya.

## Latar belakang masalah

Worker perlu melayani:

- lima domain OLTP yang berbeda;
- banyak tenant dengan kebutuhan custom masing-masing;
- import file Excel dalam jumlah besar;
- export data kandidat dan vacancy dalam jumlah besar;
- sinkronisasi data temporary ke sistem ATS;
- kebutuhan analitik tanpa membebani database transaksi.

Contoh masalah export: sistem dapat memiliki sekitar 3 juta kandidat, tetapi satu candidate
pool hanya membutuhkan sekitar 100 ribu kandidat. Export tidak seharusnya melakukan scan dan
join ulang seluruh data OLTP setiap kali user meminta file.

## Solusi arsitektur

Data diproses secara asynchronous melalui Redis Streams. Setiap workload memiliki tanggung
jawab yang jelas:

| Executable | Tanggung jawab |
| --- | --- |
| `http` | HTTP API, health check, dan publish job ke Redis. |
| `worker` | IMPORT, EXPORT, dan SYNC. |
| `worker_olap` | Extract OLTP, membangun snapshot/dimension/fact, dan aggregate analytics. |

Ketiganya tetap menggunakan codebase dan contract yang sama. Pemisahan process dilakukan agar
export berat atau proses OLAP tidak menghambat HTTP dan pekerjaan operasional lainnya.

## Gambaran arsitektur

```mermaid
flowchart TD
    Client[Client / Internal Service] -->|HTTP atau XADD| Redis[(Redis Streams)]

    subgraph Worker[Worker Exporter]
        Redis --> ImportHandler[Import Handler]
        Redis --> ExportHandler[Export Handler]
        Redis --> SyncHandler[Sync Handler]

        ImportHandler --> Import[Import Orchestrator]
        ExportHandler --> Export[Export Orchestrator]
        SyncHandler --> Sync[Sync Orchestrator]
    end

    Import --> Facilitator[Facilitator gRPC]
    Facilitator --> Excel[Excel File]
    Excel --> ImportDataset[Dataset + Validation]
    ImportDataset --> Temporary[(Temporary Schema)]

    Temporary --> Sync
    Sync --> ATS[(ATS OLTP)]

    Export --> ExportDataset[Export Dataset]
    ExportDataset --> Snapshot[(Export Snapshot)]
    Snapshot --> ChunkExport[Cursor + Chunk Processing]
    ChunkExport --> Writer[CSV / XLSX Writer]
    Writer --> Upload[Upload via gRPC]
    Upload --> Email[stream:email]

    subgraph OLAP[OLAP Worker - Target]
        Redis --> OlapWorker[OLAP Worker]
        OlapWorker --> Sources[Five OLTP Domains]
        Sources --> Extract[Incremental Extract]
        Extract --> Transform[Transform + Validate]
        Transform --> Dimensions[(Dimensions)]
        Transform --> Facts[(Facts)]
        Dimensions --> OlapWarehouse[(OLAP Warehouse)]
        Facts --> OlapWarehouse
        OlapWarehouse --> Commit[Commit + Watermark]
        Commit --> AggregateEvent[Publish Aggregate Event]
        AggregateEvent --> Redis

        Redis --> DailyWorker[Daily Aggregate Worker]
        DailyWorker --> Daily[aggregate_daily]
        Daily --> WeeklyWorker[Weekly Aggregate Worker]
        WeeklyWorker --> Weekly[aggregate_weekly]
        Weekly --> MonthlyWorker[Monthly Aggregate Worker]
        MonthlyWorker --> Monthly[aggregate_month]
        Monthly --> YearlyWorker[Yearly Aggregate Worker]
        YearlyWorker --> Yearly[aggregate_year]
    end

    Daily --> BI[Dashboard / BI / Reporting]
    Monthly --> BI
    Yearly --> BI
```

## Struktur folder arsitektur

```text
worker-exporter/
├── src/
│   ├── bin/
│   │   ├── http.rs              # HTTP API
│   │   ├── worker.rs            # IMPORT, EXPORT, dan SYNC worker
│   │   └── worker_olap.rs       # Target OLAP worker (rancangan)
│   ├── application/
│   │   ├── handler/             # Consumer handler
│   │   ├── orchestrator/        # Flow IMPORT, EXPORT, dan SYNC
│   │   ├── runner/              # Batch, cursor, dan proses job
│   │   ├── datasets/             # Dataset import/export/sync
│   │   └── *registry.rs         # Registry implementasi
│   ├── core/
│   │   ├── traits/              # Contract dan port
│   │   └── types.rs             # Shared payload dan data types
│   ├── domain/
│   │   ├── logging/             # Entity dan contract logging
│   │   ├── oltp/                # Model domain OLTP
│   │   └── snapshots/           # Model read/snapshot
│   ├── infrastructure/
│   │   ├── db/                  # Database client dan pool
│   │   ├── source/              # Source reader OLTP/warehouse
│   │   ├── loader/              # Loader warehouse dan OLTP
│   │   ├── exporter/            # CSV dan XLSX writer
│   │   ├── ingestion/           # Fetcher lintas domain
│   │   ├── pipeline/            # Pipeline teknis
│   │   ├── proto/               # Definisi gRPC
│   │   └── redis.rs             # Redis Streams adapter
│   └── utils/                   # Helper query, cursor, JSON, dan value
├── migrations/                 # Migrasi schema warehouse
├── docs/                       # Dokumentasi arsitektur dan operasional
└── Cargo.toml
```

Struktur ini memisahkan business flow, contract, domain model, dan adapter infrastructure.
Detail folder tersedia di [directory-structure.md](ARCHITECTURE.md#source-directory-structure).

## Alur utama

### IMPORT

1. Job masuk melalui `stream:import`.
2. Import orchestrator mengambil object key Excel melalui facilitator.
3. `buffer` mode membaca file dari memory; `stream` mode menyimpan file sementara ke `/tmp`.
4. Baris Excel diproses per `chunk`.
5. Dataset melakukan mapping, lookup fetcher, validasi, dan pembentukan data master/child.
6. Loader menulis hasilnya ke temporary warehouse.
7. Jika `auto_sync` aktif, worker menerbitkan job baru ke `stream:sync`.

### EXPORT

1. Job masuk melalui `stream:export`.
2. Export orchestrator memilih dataset dan writer melalui registry.
3. Dataset export mengambil data secara incremental menggunakan cursor.
4. Data diproses per `chunk` dan ditulis bertahap ke CSV/XLSX.
5. Hasil file di-upload melalui facilitator.
6. Worker dapat menerbitkan event email ke `stream:email`.

Untuk skala besar, export idealnya membaca `export snapshot`, yaitu read model yang sudah
dipersiapkan dari data terbaru. Dengan demikian, export candidate management cukup membaca
candidate pool yang diperlukan, bukan mengulang join terhadap jutaan row OLTP.

## Hasil uji concurrency awal

Uji awal dilakukan secara bersamaan dengan resource terbatas:

| Skenario | Hasil |
| --- | --- |
| 3 worker IMPORT untuk data 1.000 row | Berhasil berjalan |
| 3 worker EXPORT untuk data 500 row | Berhasil berjalan |
| Total concurrency | 3 IMPORT + 3 EXPORT |
| Resource | 1 CPU core dan 256 MB memory |

Hasil ini menunjukkan bahwa pipeline chunk, Redis Streams, dan worker masih dapat berjalan
bersamaan pada resource kecil. Angka ini adalah hasil smoke test awal, bukan batas kapasitas
produksi. Benchmark lanjutan perlu mencatat durasi job, peak memory, throughput, query database,
dan panjang antrean Redis.

### SYNC

1. Job masuk melalui `stream:sync`.
2. Sync memakai `logging_id` dari proses import.
3. Aggregator mengambil data temporary berdasarkan logging tersebut.
4. Data diproses per batch dan di-upsert ke ATS OLTP.

### OLAP

1. Job atau event masuk melalui stream OLAP.
2. Worker mengambil perubahan dari lima domain OLTP menggunakan cursor/watermark.
3. Data ditransformasi dan divalidasi.
4. Data dimuat ke dimension dan fact table.
5. Setelah transaksi berhasil di-commit, worker menerbitkan event agregasi.
6. Worker agregasi memperbarui data daily, weekly, monthly, dan yearly secara asynchronous.

## Lima logical schema data

Kelima schema ini memiliki fungsi berbeda dan tidak harus menjadi database fisik yang berbeda.

| Schema | Tujuan |
| --- | --- |
| `temporary` | Staging data import/extract sebelum validasi dan transform selesai. |
| `snapshot` | Read model berisi kondisi terbaru untuk kebutuhan export cepat. |
| `dimensions` | Data master seperti tenant, candidate, vacancy, location, dan entity referensi. |
| `facts` | Data transaksi pada grain yang jelas, bersumber dari event/transaksi OLTP. |
| `aggregate` | Hasil analitik daily, weekly, monthly, dan yearly. |

`snapshot` bukan pengganti fact atau aggregate. Snapshot dibuat untuk kebutuhan baca cepat dan
spesifik export; aggregate dibuat untuk kebutuhan analitik.

Untuk snapshot “data terbaru”, `GROUP BY` saja tidak cukup. Proyeksi snapshot perlu menggunakan
upsert berdasarkan business key dan version/`updated_at`, atau pola seperti:

```sql
ROW_NUMBER() OVER (
    PARTITION BY tenant_id, vacancy_id, candidate_pool_id, candidate_id
    ORDER BY updated_at DESC
)
```

## Multi-tenant dan customization

Setiap data yang tenant-scoped membawa identitas tenant, minimal:

```text
tenant_id
source_id
job_id / logging_id
updated_at
```

Customization tenant tidak diletakkan sebagai percabangan besar di handler atau orchestrator.
Tenant dapat diarahkan ke implementasi melalui registry:

```text
Tenant
  ↓
Dataset Registry
  ↓
Pipeline Registry
  ↓
Warehouse Loader Registry
```

Contoh extension point:

- `Orchestrator Registry` memilih flow `IMPORT`, `EXPORT`, atau `SYNC`;
- `export_config.dataset` memilih dataset export;
- `export_config.format` memilih writer CSV/XLSX;
- `import_config.dataset` memilih mapper import;
- `import_config.pipeline` memilih pipeline;
- `import_config.loader` memilih loader warehouse;
- `sync_config.sync_aggregate` memilih aggregator sync.

Dengan pola ini, kebutuhan tenant baru cukup menambahkan implementasi dan mendaftarkan key-nya.
Handler, orchestrator, dan flow utama tetap digunakan bersama. Ini tetap membutuhkan code
change dan deployment, tetapi tidak membutuhkan perubahan pada seluruh sistem.

## Registry Pattern

Registry menyimpan implementasi berdasarkan key string dan mengembalikan trait object yang
sesuai. Orchestrator mengetahui contract, bukan detail setiap adapter.

```text
Registry<Key, Implementation>
```

Urutan registry dalam flow worker:

```text
Redis Stream
  ↓
Orchestrator Registry
  ↓
Dataset / Pipeline / Loader / Writer Registry
  ↓
Database, file, atau service eksternal
```

`Orchestrator Registry` menjadi entry point flow bisnis. Saat ini key utamanya adalah
`IMPORT`, `EXPORT`, dan `SYNC`. Registry di bawahnya menyediakan implementasi spesifik tanpa
membuat handler baru untuk setiap dataset atau tenant.

Manfaatnya:

- dataset baru tidak perlu membuat handler baru;
- format export baru tidak perlu mengubah export orchestrator;
- loader tenant-specific dapat ditambahkan tanpa mengubah alur import;
- source dan pipeline dapat ditukar melalui konfigurasi job.

Trade-off-nya adalah key string harus valid. Key yang salah baru terlihat saat runtime, sehingga
registry perlu divalidasi saat startup dan payload perlu memiliki validation yang jelas.

## Chunk dan stream processing

### Import

- `buffer`: lebih sederhana untuk file kecil, tetapi seluruh file berada di memory;
- `stream`: lebih cocok untuk file besar karena file diproses melalui temporary file;
- `chunk`: membatasi jumlah row yang dipetakan dan dimuat dalam satu batch.

### Export

- cursor menghindari offset besar pada database;
- chunk membatasi row yang diproses per iterasi;
- writer menulis row secara bertahap ke CSV/XLSX;
- snapshot menghindari join besar ketika user meminta export.

## Skalabilitas

Desain ini dapat scale secara horizontal karena Redis Consumer Group membagi job antar process.
Pemisahan `http`, `worker`, dan `worker_olap` juga memungkinkan setiap workload memiliki jumlah
replica dan resource yang berbeda.

Hal yang harus diperkuat sebelum production scale besar:

1. ACK hanya setelah job sukses, dengan retry terbatas dan dead-letter queue.
2. Pending Redis message recovery menggunakan mekanisme claim/reclaim.
3. Consumer name unik per instance.
4. Idempotency untuk import, snapshot, fact, dan aggregate.
5. Outbox atau mekanisme event setelah transaksi dimension/fact berhasil commit.
6. Index, partitioning, dan query plan berbasis `tenant_id` serta filter export utama.
7. Rate limit atau resource quota agar tenant besar tidak menjadi noisy neighbor.
8. Cleanup file export dan graceful shutdown worker.

## Penilaian arsitektur

Arsitektur ini paling tepat disebut **modular, event-driven monolith**. Ia belum menjadi
microservice penuh karena masih berbagi codebase, contract, dan infrastructure, tetapi dapat
dijalankan sebagai beberapa process/service yang di-scale secara independen.

Kompleksitasnya wajar untuk sistem yang memiliki lima domain OLTP, multi-tenant customization,
import/export, sinkronisasi, snapshot read model, dan OLAP. Fokus pengembangan sebaiknya bukan
menambah lebih banyak layer, melainkan menjaga batas tanggung jawab, reliability worker, dan
konsistensi data.

## Nilai project

Project ini menunjukkan kemampuan untuk:

- merancang arsitektur worker asynchronous berbasis Rust/Tokio;
- mengintegrasikan Redis Streams, PostgreSQL, gRPC, CSV, dan XLSX;
- membuat pipeline import/export yang berjalan dengan chunk dan cursor;
- merancang read model snapshot untuk export berskala besar;
- mendukung lima domain OLTP dan customization multi-tenant;
- menggunakan registry dan trait untuk extension point yang terkontrol;
- memisahkan workload operasional dan analitik tanpa menggandakan codebase.

## Ringkasan untuk LinkedIn

Saya membangun sebuah data worker berbasis Rust untuk menangani import, export, sinkronisasi,
dan kebutuhan OLAP pada sistem multi-tenant dengan lima domain OLTP.

Untuk menjaga performa, data import dan extract diproses secara asynchronous melalui Redis
Streams. Export menggunakan cursor, chunk processing, dan snapshot read model agar tidak perlu
melakukan scan serta join terhadap jutaan data setiap kali user meminta file.

Customization tenant dilakukan melalui registry dataset, pipeline, dan warehouse loader tanpa
mengubah handler atau orchestrator utama. Ke depan, worker OLAP akan membangun dimension dan fact
table, kemudian memperbarui aggregate daily, weekly, monthly, dan yearly melalui event terpisah.

Project ini dibangun sebagai modular monolith multi-process: satu codebase, tetapi workload HTTP,
operasional worker, dan OLAP dapat di-scale secara independen.

> Modularitas tidak selalu berarti memecah semuanya menjadi microservices. Kadang yang lebih
> penting adalah memisahkan tanggung jawab, alur data, dan beban kerja dengan jelas.

## Teknologi

`Rust` · `Tokio` · `Redis Streams` · `PostgreSQL` · `SQLx` · `Axum` · `gRPC/Tonic` · `CSV` ·
`XLSX` · `Docker`

---

## Source: README-validation-registry (1)

zsh:1: no matches found: (1).md

---

## Source: README

# Dokumentasi Worker Exporter

`worker-exporter` adalah layanan Rust berbasis Tokio yang menjalankan pekerjaan asinkron melalui Redis Streams. Implementasi yang siap dipakai saat ini mencakup proses **IMPORT**, **EXPORT**, dan **SYNC**, serta HTTP API untuk health check, logging, file download, dan pembuatan template Excel.

Dokumentasi ini ditulis dari implementasi kode saat ini. Nama registry, payload, dan perilaku operasional di bawah adalah kontrak aktual aplikasi, bukan rancangan target.

## Mulai dari sini

1. [Menjalankan aplikasi](ARCHITECTURE.md#source-setup-and-running) — prasyarat, environment variable, migrasi, dan pilihan executable.
2. [Panduan penggunaan](ARCHITECTURE.md#source-usage) — endpoint HTTP, payload Redis, serta contoh IMPORT, EXPORT, dan SYNC.
3. [Arsitektur](ARCHITECTURE.md#source-architecture) — alur runtime, layer, registry, dan integrasi eksternal.
4. [Struktur direktori](ARCHITECTURE.md#source-directory-structure) — fungsi tiap folder dan berkas penting.
5. [Operasional dan pengembangan](ARCHITECTURE.md#source-operations-and-development) — observabilitas, delivery Redis, batasan, dan cara memperluas fitur.

## Pilihan executable

| Perintah | Fungsi | Kapan dipakai |
| --- | --- | --- |
| `cargo run --bin http_worker` | Menjalankan HTTP API dan worker dalam satu proses. | Pilihan lokal dan container default. |
| `cargo run --bin worker` | Menjalankan tiga worker Redis: IMPORT, EXPORT, dan SYNC. | Deployment worker terpisah. |
| `cargo run --bin http` | Menjalankan HTTP API tanpa worker. | Deployment API terpisah. |
| `cargo run --bin worker-exporter` | Memanggil bootstrap lalu berhenti setelah sekitar 10 detik. | Bukan entry point service untuk production. |

`http_worker` membuat satu worker `INGESTION` dan tiga worker untuk tiap flow EXPORT, IMPORT, serta SYNC. Namun orchestrator `INGESTION` belum didaftarkan; karena itu flow tersebut belum dapat digunakan end-to-end. Detailnya ada di [arsitektur](ARCHITECTURE.md#status-implementasi-dan-batasan).

## Ringkasan kemampuan

| Flow | Stream | Status | Hasil utama |
| --- | --- | --- | --- |
| IMPORT | `stream:import` | Aktif | File Excel dipetakan dan dimuat ke tabel temporary warehouse. |
| EXPORT | `stream:export` | Aktif | CSV/XLSX dibuat, diunggah lewat facilitator, lalu event email dipublikasikan. |
| SYNC | `stream:sync` | Aktif | Data temporary vacancy diproses dan di-upsert ke ATS OLTP. |
| INGESTION | `stream:ingestion` | Belum siap | Consumer group dibuat, tetapi orchestrator tidak tersedia. |

## Dokumen lama

`ID_ARCHITECTUE_MD.md`, `EN_ARCHITECTURE_MD.md`, dan `ARCHICTECTURE.MD` adalah dokumen lama yang tetap dipertahankan agar tautan historis tidak putus. Gunakan rangkaian dokumen ini sebagai referensi operasional utama.

---

## Source: architecture

# Arsitektur

## Gambaran umum

Worker menerima pekerjaan dari Redis Streams, memilih orchestrator berdasarkan stream yang dikonsumsi, lalu menjalankan pipeline untuk import, export, atau sinkronisasi. Semua dependency dibuat sekali pada `bootstrap()` dan dibagikan lewat `Arc<AppState>`.

```mermaid
flowchart LR
    Client[Client / service lain] -->|HTTP atau XADD payload| Redis[(Redis Streams)]
    Client -->|HTTP API| Api[http atau http_worker]

    Redis --> Import[stream:import]
    Redis --> Export[stream:export]
    Redis --> Sync[stream:sync]

    Import --> IH[ImportHandler]
    Export --> EH[ExportHandler]
    Sync --> SH[SyncHandler]

    IH --> IO[ImportOrchestrator]
    EH --> EO[ExportOrchestrator]
    SH --> SO[SyncOrchestrator]

    IO --> S3[Facilitator gRPC]
    IO --> Warehouse[(Warehouse PostgreSQL)]
    EO --> Dataset[Export dataset]
    Dataset --> ATS[(ATS PostgreSQL)]
    EO --> File[exports/*.csv atau *.xlsx]
    File --> S3
    EO --> Email[stream:email]
    SO --> Warehouse
    SO --> ATS
```

## Bootstrap dan dependency

`src/boostrap.rs` adalah composition root aplikasi. Urutan utamanya:

1. Membaca environment melalui `Config`.
2. Membuat `ConnectionManager` Redis dan `RedisPublisher`.
3. Membuka pool PostgreSQL untuk warehouse, organization, ATS, master, dan HRIS.
4. Membuat client database dan client gRPC facilitator/ATS.
5. Menjalankan embedded migration pada pool warehouse.
6. Mendaftarkan source, dataset, fetcher, loader, writer, dan orchestrator.
7. Menyusun `AppState` yang dipakai handler HTTP dan consumer Redis.

`http_worker` kemudian membuat consumer group Redis dan menyalakan handler. `worker` melakukan hal serupa, tetapi tanpa HTTP API.

## Layer aplikasi

| Layer | Lokasi | Tanggung jawab |
| --- | --- | --- |
| Entrypoint | `src/bin/` | Menjalankan HTTP API, worker, atau keduanya. |
| Application | `src/application/` | Handler Redis, orchestrator, runner, registri, use case HTTP, dan koordinasi flow. |
| Core | `src/core/` | Kontrak trait, tipe payload, konfigurasi, dan registry generik. |
| Domain | `src/domain/` | Entitas serta kontrak domain, terutama logging. |
| Infrastructure | `src/infrastructure/` | Redis adapter, database client/source/loader, exporter file, dan gRPC proto. |
| Utilities | `src/utils/` | Resolver export, filter/fetcher/cache, JSON path, custom-field, query, dan cursor. |

Kode bergantung pada trait dari `core`; implementasi teknis ditempatkan di `infrastructure`. Sebagai contoh, `Dataset`, `WritterTrait`, `SourceIncrementalExtract`, `ImportLoadPersitence`, dan `BulkLoaderTrait` didefinisikan sebagai kontrak lalu diisi oleh adapter infrastruktur.

## Pola registry

`Registry<T>` di `src/core/traits/strategy/registry.rs` menyimpan implementasi berdasarkan key string. Pola ini adalah titik ekstensi utama. Key dalam payload harus persis sama dengan key yang didaftarkan saat bootstrap.

| Registry | Contoh key aktif | Dipakai oleh |
| --- | --- | --- |
| Orchestrator | `IMPORT`, `EXPORT`, `SYNC`, `ROLLBACK`, `AGGREGATE` | Handler berdasarkan stream. |
| Writer | `csv`, `excel`, `excel_rs` | `export_config.format`. `excel` memakai writer lama; `excel_rs` memakai `rust_xlsxwriter`. Keduanya menghasilkan `.xlsx`. |
| Export dataset | `lsi_ats_candidate_dataset`, `lsi_applicant_dataset`, `lsi_reference_check_dataset` | `export_config.dataset`. |
| Import dataset v1 | `DYNAMIC_DATASET_V1`, `LSI_IMPORT_VACANCY_V1` | `import_config.dataset`. |
| Import loader | `WAREHOUSE_ATS_VACANCY_TEMPORARY`, `WAREHOUSE_DYNAMIC_TEMPORARY_LOADER` | `import_config.loader`. |
| Import pipeline | `vacancy_import_pipeline` | `import_config.pipeline`. |
| Sync aggregator | `LSI_VACANCY_SYNC` | `sync_config.sync_aggregate`. |

Source dan data fetcher juga memakai registry, tetapi sebagian besar dipanggil internal oleh dataset/aggregator. Lihat [struktur direktori](ARCHITECTURE.md#source-directory-structure) dan [pengembangan](ARCHITECTURE.md#menambah-kemampuan-baru).

## Flow runtime

### IMPORT

1. `ImportHandler` mengambil `payload` dari `stream:import` dan memilih orchestrator `IMPORT`.
2. `ImportOrchestrator` membuat record logging berstatus `PROCESSING` di warehouse.
3. Object key pada `import_config.key` diterjemahkan menjadi URL unduhan melalui facilitator gRPC.
4. Buffer mode membaca Excel langsung dari memory. Stream mode mengunduh response bertahap ke `/tmp/worker-exporter-import-<uuid>.xlsx`, membuka workbook dari file tersebut, lalu menghapus file sementara setelah proses selesai.
5. Sheet pertama dibaca dan diproses per `chunk` mulai `start_row` (default baris 2). Fetcher reference diprefetch secara batch per chunk ke cache in-memory ber-TTL 30 menit dan kapasitas maksimum 5.000 entry.
6. Dataset import memetakan setiap baris ke master/child/custom-field lalu loader menulisnya ke tabel temporary warehouse. Lookup lanjutan menggunakan helper cache untuk normalisasi key, operator casting, cache hit/miss, dan logging.
7. Jika `auto_sync` bernilai `true`, payload SYNC baru dipublikasikan ke `stream:sync` dengan `logging_id` hasil import.

### EXPORT

1. `ExportHandler` memilih orchestrator `EXPORT`.
2. `ExportOrchestrator` menyimpan logging awal dan memilih writer serta dataset dari registry.
3. Filter `tenant_id` selalu diterapkan; `filters` dari payload ditambahkan sesudahnya.
4. `ExportRunner` membaca dataset secara cursor-based per `export_config.chunk`, meneruskan konfigurasi field serta cache bersama ke dataset, menulis header dan baris ke CSV/XLSX, lalu memperbarui progress logging.
5. Berkas sementara dibuat di `exports/<stream-id-yang-disanitasi>.<ext>`.
6. Berkas diunggah dengan facilitator gRPC. Key hasil upload dicatat sebagai `download_url` dan event email dikirim ke `stream:email`.

`lsi_reference_check_dataset` mengekspor header reference check dari data kandidat ATS dan
custom field EAV. Architecture, penggunaan, dan contoh config ada di
[reference check dan operasi](ARCHITECTURE.md#source-reference-check-and-operations).

### ROLLBACK

1. `POST /rollback` memvalidasi metadata job dan memasukkan payload ke `stream:rollback`.
2. `RollbackHandler` mengambil pesan dan memilih `RollbackOrchestrator`.
3. Orchestrator membuat logging `ROLLBACK`, mengambil pipeline dari registry, lalu menjalankan
   loader child sebelum parent dalam satu transaksi ATS.
4. Jika satu loader gagal, transaksi dibatalkan dan logging rollback menjadi `FAILED`.

### Reusable dataset utilities

Dataset import dan export tidak lagi menyimpan seluruh helper lookup di masing-masing file.
Helper lintas dataset berada di `src/utils/`:

| Utility | Peran |
| --- | --- |
| `fetcher.rs` | Normalisasi token, operator filter, filter UUID, cache key, single fetch, batch fetch, dan logging cache. |
| `json_value.rs` | Resolusi JSON path, flatten array, konversi keyword, unique string values, dan index row. |
| `custom_field.rs` | Resolusi nilai EAV, grouping custom field per parent, dan index row custom field. |
| `export_resolver.rs` | Resolusi column path terhadap beberapa source JSON dan format nilai export. |

`ApplicantDataset`, `LsiCandidateDataset`, `DynamicDatasetV1`, dan `LsiImportVacancyV1`
menggunakan helper tersebut. Aturan bisnis seperti source utama applicant, hubungan vacancy,
job grade, dan work location tetap berada di dataset masing-masing. `SyncVacancyDataset` yang
ditinjau hanya berisi struct kosong, sehingga tidak dipaksa memakai helper yang tidak relevan.

### SYNC

1. `SyncHandler` memilih orchestrator `SYNC`.
2. `SyncOrchestrator` mewajibkan `sync_config` dan `logging_id`, lalu mengubah status logging menjadi `SYNC_PROGRESS`.
3. Aggregator `LSI_VACANCY_SYNC` mengambil vacancy temporary yang cocok dengan `logging_id`.
4. Dataset sync menyusun master beserta child data. Lima bulk loader menulis vacancy, custom field, PIC, stage, dan requirement dalam transaksi ATS per batch.
5. Status logging menjadi `SYNC_SUCCESS` atau `SYNC_FAILED`.

### Warehouse migration

`WAREHOUSE_MIGRATOR` di `src/boostrap.rs` menjalankan folder `migrations/` ke pool warehouse.
Migration di-embed ke binary, sehingga Docker tidak perlu meng-install `sqlx-cli`. Pool ATS,
organization, master, dan HRIS tidak menjalankan migration tersebut.

## Logging dan observabilitas data

Tabel `loggings` di warehouse menyimpan identitas job, tenant, email, status, error, cursor, jumlah baris, key file unduhan, dan konfigurasi job. Untuk IMPORT, `store()` mengembalikan ID logging baru; ID itu kemudian menjadi filter utama flow SYNC.

Status yang digunakan kode meliputi `PROCESSING`, `COMPLETED`, `FAILED`, `SYNC_PROGRESS`, `SYNC_SUCCESS`, dan `SYNC_FAILED`. Endpoint `POST /loggings` menyediakan daftar logging per tenant.

## Delivery Redis

Consumer membaca sampai 10 pesan dengan `XREADGROUP` dan `BLOCK 100`. Setelah handler selesai — termasuk bila JSON invalid atau orchestrator menghasilkan error — pesan di-ACK.

Konsekuensinya, delivery aplikasi saat ini bukan retry otomatis. Kesalahan job dicatat pada log aplikasi dan, bila flow sudah sempat membuat record, pada tabel `loggings`; pesan tidak tetap pending untuk dicoba ulang. Selain itu, consumer selalu membaca ID `>` sehingga pending message lama tidak direclaim oleh worker ini. Rincian mitigasi operasional ada di [operasional](ARCHITECTURE.md#delivery-dan-penanganan-kegagalan).

## Status implementasi dan batasan

- IMPORT, EXPORT, dan SYNC telah didaftarkan dan dipanggil oleh handler masing-masing.
- Consumer group `stream:ingestion` dibuat oleh semua runtime worker, tetapi `IngestionHandler` meminta key `ingestion` (huruf kecil) yang tidak ada dalam `OrchestratorRegistry`. Jangan mengirim job production ke stream ini.
- `worker` tidak menyalakan `IngestionHandler`; `http_worker` menyalakannya, tetapi flow tetap gagal karena registry tersebut.
- Semua konfigurasi database dan URL gRPC bersifat wajib pada startup, termasuk konfigurasi yang belum tentu dipakai setiap job.
- Stream IMPORT memerlukan URL download absolut dan permission tulis ke `/tmp`; file sementara import dibersihkan setelah proses, tetapi kegagalan permission dapat terlihat sebagai warning cleanup.
- File export tidak dihapus setelah sukses karena penghapusan di `ExportOrchestrator` masih dikomentari. Pastikan direktori `exports/` dipantau dan dibersihkan dengan kebijakan deployment.

---

## Source: config-driven-workflows

# Config-driven import, sync, and rollback

Ketiga pekerjaan berjalan mandiri lewat Redis stream: `stream:import`, `stream:sync`, dan `stream:rollback`. Semua nama dataset, pipeline, source, dan loader adalah key registry aplikasi; payload tidak dapat menentukan nama tabel SQL atau menjalankan query bebas.

## Dynamic import

Gunakan `DYNAMIC_IMPORT_DATASET` untuk mapping Excel yang sepenuhnya mengikuti `import_config.fields`, `master_table`, dan `sequential_table`. Pipeline `CONFIG_IMPORT_PIPELINE` mengeksekusi setiap key pada `loaders` secara berurutan. `loader` tetap didukung sebagai kompatibilitas untuk satu loader.

```json
{
  "type": "IMPORT",
  "tenant_id": "tenant-uuid",
  "email": "user@example.com",
  "actor_id": "actor-uuid",
  "orchestrator_name": "IMPORT",
  "import_config": {
    "dataset": "DYNAMIC_IMPORT_DATASET",
    "key": "s3-file-key",
    "data_source_type": "stream",
    "chunk": 250,
    "tenant_id": "tenant-uuid",
    "group_id": "group-uuid",
    "auto_sync": false,
    "pipeline": "CONFIG_IMPORT_PIPELINE",
    "loaders": ["WAREHOUSE_DYNAMIC_TEMPORARY_LOADER"],
    "master_table": "warehouse.ats_vacancy_temporary",
    "primary_column": "id",
    "sequential_table": [
      {
        "table": "warehouse.ats_vacancy_stage_temporary",
        "is_custom": false,
        "fk_column": "vacancy_id"
      }
    ],
    "fields": [
      {"header": "Name", "column": "warehouse.ats_vacancy_temporary:name", "fetcher": null, "is_custom": false},
      {"header": "Stage", "column": "warehouse.ats_vacancy_stage_temporary:stage_name", "fetcher": null, "is_custom": false}
    ]
  }
}
```

## Config sync pipeline

`CONFIG_SYNC_PIPELINE` mengambil data temporary dengan `logging_id`, memformat melalui `dataset`, kemudian menjalankan semua bulk loader dalam satu transaksi per batch. Urutan `loaders` dipertahankan.

```json
{
  "type": "SYNC",
  "tenant_id": "tenant-uuid",
  "email": "user@example.com",
  "actor_id": "actor-uuid",
  "orchestrator_name": "SYNC",
  "sync_config": {
    "sync_aggregate": "CONFIG_SYNC_PIPELINE",
    "logging_id": "import-logging-uuid",
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

## Rollback OLTP

Rollback hanya menghapus record OLTP yang `logging_id`-nya sama. Loader child harus ditempatkan sebelum loader parent. Semua delete dijalankan dalam satu transaksi; kegagalan satu loader membatalkan seluruh rollback.

```json
{
  "type": "ROLLBACK",
  "tenant_id": "tenant-uuid",
  "email": "user@example.com",
  "actor_id": "actor-uuid",
  "orchestrator_name": "ROLLBACK",
  "rollback_config": {
    "pipeline": "CONFIG_ROLLBACK_PIPELINE",
    "logging_id": "import-logging-uuid",
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

Rollback membuat logging baru dengan type `ROLLBACK`; import/sync logging target tidak diubah.

## Aggregate candidate

Migration `20260814090000_create_aggregate_period.up.sql` harus dijalankan pada database ATS. Pipeline `CANDIDATE_AGGREGATE` membaca `candidate.candidate.created_at` sekali, lalu menyimpan period `DAILY`, `MONTHLY`, dan `YEARLY` ke tabel yang sama: `aggregate_period`.

```json
{
  "type": "AGGREGATE",
  "tenant_id": "tenant-uuid",
  "email": "system@example.com",
  "actor_id": "system",
  "orchestrator_name": "AGGREGATE",
  "aggregate_config": {
    "pipeline": "CANDIDATE_AGGREGATE",
    "aggregate_code": "CANDIDATE_TOTAL",
    "scope_key": "GLOBAL",
    "from": "2026-01-01",
    "to": "2027-01-01"
  }
}
```

`from` inclusive dan `to` exclusive. Keduanya boleh dihilangkan untuk seluruh data. Saat ini source candidate belum memiliki `tenant_id`, sehingga aggregate hanya menerima `scope_key: GLOBAL`; filter tenant dapat ditambahkan setelah kolom sumber tersedia.

Loader OLAP vacancy mem-publish `VACANCY_AGGREGATE` dengan `aggregate_code: VACANCY_TOTAL`, sehingga hasilnya tersimpan dengan `entity_type: vacancy`.

---

## Source: directory-structure

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
| `writter.rs` | Factory writer `csv`, `excel`, dan `excel_rs`. Ejaan `writter` mengikuti kode saat ini. |
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

---

## Source: job-configuration

# Konfigurasi job Redis

Dokumen ini menjelaskan dua object yang mengendalikan pekerjaan utama worker:
`export_config` untuk membuat berkas dan `import_config` untuk membaca Excel. Keduanya adalah
bagian dari JSON `OrchestratorJob` pada field Redis `payload`.

Routing tidak memakai `orchestrator_name`. Stream menentukan handler yang dipakai:

| Stream | Konfigurasi yang diperlukan |
| --- | --- |
| `stream:export` | `export_config` |
| `stream:import` | `import_config` |
| `stream:sync` | `sync_config` |
| `stream:rollback` | `rollback_config` |

`tenant_id`, `email`, `actor_id`, dan `orchestrator_name` tetap harus ada pada root payload.
Untuk contoh payload lengkap, lihat [panduan penggunaan](ARCHITECTURE.md#source-usage).

## `export_config`

EXPORT memilih dataset dan writer dari registry, membaca data secara cursor-based, membuat
CSV/XLSX di direktori `exports/`, mengunggah hasilnya lewat facilitator gRPC, lalu opsional
mengirim event email.

| Field | Wajib | Arti dan perilaku aktual |
| --- | --- | --- |
| `dataset` | Ya | Key export dataset. Saat ini: `lsi_ats_candidate_dataset`, `lsi_applicant_dataset`, `lsi_reference_check_dataset`, atau alias `APPLICANT_DATASET`. Key yang tidak terdaftar membuat job gagal. |
| `format` | Ya | Writer yang dipakai: `csv`, `excel`, atau `excel_rs`. `excel` dan `excel_rs` menghasilkan ekstensi `.xlsx`. |
| `fields` | Ya | Daftar kolom keluaran, dalam urutan yang sama dengan berkas export. |
| `template_code` | Ya | Kode template email. Struct meminta field ini meskipun email dimatikan. |
| `structure_template_code` | Ya | Kode struktur template email; juga tetap wajib di payload. |
| `redirected_url` | Ya | URL yang dicatat/dikirim setelah upload. Gunakan `{{url}}` sebagai placeholder key object dari facilitator. |
| `chunk` | Ya | Jumlah row yang dibaca per batch. Gunakan integer positif; naikkan bertahap sambil memantau memory dan database. |
| `send_email` | Tidak | Hanya nilai `true` yang menerbitkan event ke `stream:email`. `false` atau field yang tidak ada berarti tidak ada event email. |

Setiap item `fields` memakai format berikut:

| Field kolom | Wajib | Arti |
| --- | --- | --- |
| `name` | Ya | Judul kolom pada CSV/XLSX. |
| `column` | Tidak | Path data yang diminta dataset, misalnya `id`, `candidateName`, atau `vacancy.name`. `null` menghasilkan `-` pada dataset applicant. |
| `fetcher` | Tidak | Nama lookup tambahan. Dukungan detailnya ditentukan oleh dataset; dataset applicant memakai fetcher internal seperti `MASTER_GEO` dan `JOB_GRADE`. |
| `fetcher_field_filter`, `fetcher_field_key`, `fetcher_key` | Tidak | Metadata relasi lookup. Saat ini tidak semua dataset menafsirkan metadata ini secara dinamis. |
| `is_cache` | Tidak | Aktifkan cache lookup untuk field yang didukung dataset. Default `false`. |

Contoh minimal:

```json
{
  "dataset": "lsi_ats_candidate_dataset",
  "format": "csv",
  "fields": [
    {"name": "Candidate ID", "column": "id"},
    {"name": "Name", "column": "name"}
  ],
  "template_code": "EMAIL_TEMPLATE_CODE",
  "structure_template_code": "EMAIL_STRUCTURE_CODE",
  "redirected_url": "https://app.example.com/download?url={{url}}",
  "chunk": 500,
  "send_email": true
}
```

Filter tambahan bukan bagian dari `export_config`, melainkan field root `filters`. Worker selalu
menambahkan filter tenant dari `tenant_id` root job.

## `import_config`

IMPORT meminta URL unduhan untuk `key` kepada facilitator, membaca sheet pertama Excel, lalu
memproses row per `chunk`. Dataset mapper membentuk data master/child/custom field dan loader
menulis hasilnya ke tabel temporary warehouse.

| Field | Wajib | Arti dan perilaku aktual |
| --- | --- | --- |
| `key` | Ya | Object key Excel yang dikirim ke facilitator untuk memperoleh URL download. Bukan path file lokal. |
| `data_source_type` | Ya | Tepat `buffer` membaca seluruh file di memory. Nilai lain, misalnya `stream`, mengunduh ke `/tmp` dahulu lalu memproses file. |
| `dataset` | Ya | Key mapper import. Saat ini: `DYNAMIC_DATASET_V1` atau `LSI_IMPORT_VACANCY_V1`. |
| `loader` | Ya | Key loader temporary warehouse: `WAREHOUSE_ATS_VACANCY_TEMPORARY` atau `WAREHOUSE_DYNAMIC_TEMPORARY_LOADER`. |
| `pipeline` | Ya | Key pipeline yang harus terdaftar, saat ini `vacancy_import_pipeline`. Kode saat ini me-resolve key ini, tetapi pipeline belum menjalankan transform tambahan; jangan mengandalkannya untuk logika bisnis baru. |
| `tenant_id` | Ya | Tenant yang ditulis ke data import. Sebaiknya sama dengan `tenant_id` pada root job. |
| `group_id` | Ya | Group yang diteruskan oleh dataset vacancy. Tetap isi untuk kontrak payload. |
| `master_table` | Ya | Nama tabel/entity utama hasil mapping. Alias JSON `main_table` juga diterima. |
| `primary_column` | Ya | Kolom ID utama untuk row master yang dibuat mapper. |
| `fields` | Ya | Definisi kolom Excel dan tujuan mappingnya. Dibahas pada tabel berikut. |
| `chunk` | Ya | Jumlah row Excel per batch. Gunakan integer positif. |
| `auto_sync` | Ya | Bila `true`, IMPORT mempublikasikan job SYNC setelah sukses. Isi `sync_config.sync_aggregate`; `logging_id` akan diisi worker. |
| `start_row` | Tidak | Nomor baris Excel mulai dari 1. Default `2`, sehingga header di baris pertama dilewati. |
| `sequential_table` | Tidak | Tabel child untuk dataset/loader dynamic. Tiap item berisi `table`, `is_custom`, dan `fk_column`. |
| `fetcher_caching` | Tidak | Daftar key fetcher yang boleh memakai cache pada `DYNAMIC_DATASET_V1`. Tidak dipakai oleh mapper `LSI_IMPORT_VACANCY_V1`. |
| `main_filterable` | Tidak | Konfigurasi `filterable_attribute` untuk `LSI_IMPORT_VACANCY_V1`. Tidak berlaku untuk semua dataset. |
| `main_searchable` | Tidak | Daftar path yang digabung menjadi `keywords` pada `LSI_IMPORT_VACANCY_V1`. |
| `permission_key` | Tidak | Daftar permission yang ditulis menjadi `permission_keys` oleh `LSI_IMPORT_VACANCY_V1`. Berbeda dari `permissions_key` di root job. |

Setiap item `fields` memetakan satu header Excel:

| Field kolom | Wajib | Arti |
| --- | --- | --- |
| `header` | Ya | Nama header Excel. Pada mode stream, header dicocokkan berdasarkan nama, bukan posisi kolom. |
| `column` | Ya | Nama/path kolom tujuan. Sintaks dan tabel tujuan ditentukan dataset yang dipilih. |
| `fetcher` | Tidak | Key lookup untuk mengubah nilai Excel menjadi data referensi. |
| `fetcher_field_filter`, `fetcher_field_key`, `fetcher_key` | Tidak | Cara fetcher mencocokkan nilai input dan mengambil ID/nilai hasilnya. Isi hanya sesuai kontrak fetcher yang dipakai. |
| `type_data` | Tidak | Tipe nilai: `TEXT`, `UUID`, `DATE`, `NUMBER`, atau `Boolean`. |
| `casting` | Tidak | Casting untuk query fetcher: `Uuid`, `Varchar`, `Date`, `Number`, atau `{"Enum":"nama_enum"}`. |
| `is_custom` | Ya | Menandai mapping custom field/EAV. |
| `validation_rules` | Tidak | Validasi mapper LSI. Alias `validations`, `validation_config`, dan `validationConfig` diterima. Contoh validator terdaftar: `REQUIRED`, `UNIQUE`. |
| `template` | Tidak | Metadata template; tidak dipakai oleh proses import utama saat ini. |
| `select`, `is_child` | Tidak | Metadata untuk menampilkan detail hasil import, bukan pengatur utama mapping Excel. |

Contoh field yang melakukan lookup:

```json
{
  "header": "Job Position",
  "column": "vacancy:position_id",
  "fetcher": "JOB_POSITION",
  "type_data": "UUID",
  "is_custom": false,
  "fetcher_field_filter": null,
  "fetcher_field_key": "id",
  "fetcher_key": "id",
  "casting": "Uuid",
  "validation_rules": [{"validator": "REQUIRED"}]
}
```

## Pilihan yang aman

- Mulai dari `chunk` 100–500, lalu ukur database, memory, dan lama transaksi sebelum menaikkan.
- Gunakan `data_source_type: "stream"` untuk file besar agar file tidak seluruhnya berada di memory.
- Jangan memasukkan key registry sembarang; key harus sama dengan yang didaftarkan aplikasi.
- Jika `auto_sync` aktif, pastikan sync target siap. IMPORT sukses hanya berarti data temporary berhasil ditulis; hasil final ATS ditentukan flow SYNC.

---

## Source: operations-and-development

# Operasional dan pengembangan

## Delivery dan penanganan kegagalan

Consumer memakai `XREADGROUP` dengan ID `>` dan melakukan `XACK` setelah setiap handler diproses. ACK tetap dilakukan jika:

- JSON `payload` tidak dapat dideserialisasi;
- orchestrator tidak ditemukan;
- flow mengembalikan error.

Artinya, pesan tidak memiliki retry otomatis di level worker. Untuk job yang gagal:

1. Cari `msg_id`/`logging_id` pada log aplikasi atau `POST /loggings`.
2. Perbaiki konfigurasi, object Excel, data referensi, atau target database.
3. Kirim pesan baru dengan payload yang telah diperbaiki.

Jangan mengandalkan `XPENDING` sebagai mekanisme retry bawaan: worker tidak melakukan `XAUTOCLAIM` atau pembacaan pending entry. Pending message hanya mungkin muncul jika process terhenti sebelum ACK atau ACK Redis gagal, dan perlu ditangani dengan runbook/consumer khusus.

## Memantau Redis

```bash
redis-cli -u "$REDIS_URL" XINFO GROUPS stream:import
redis-cli -u "$REDIS_URL" XPENDING stream:import import-group
redis-cli -u "$REDIS_URL" XINFO GROUPS stream:export
redis-cli -u "$REDIS_URL" XINFO GROUPS stream:sync
```

Hal yang perlu diperiksa:

- `lag` meningkat: worker kurang kapasitas, macet, atau producer terlalu cepat.
- `pending` meningkat: consumer berhenti/terputus sebelum ACK.
- job tak terlihat di logging: payload mungkin invalid atau gagal sebelum logging dibuat.
- job export berhasil tetapi email tidak diterima: periksa consumer untuk `stream:email`; worker ini hanya mempublikasikan event.

Setiap instance `http_worker` menciptakan tiga consumer untuk masing-masing flow IMPORT, EXPORT, dan SYNC. Menambah instance akan menambah concurrency dalam consumer group yang sama. Atur `chunk` secara konservatif saat meningkatkan jumlah instance karena import dan sync sama-sama memakai koneksi serta transaksi database.

## Logging aplikasi

Logging menggunakan `tracing` dengan filter dari `RUST_LOG`; defaultnya `info,sqlx=debug` apabila `RUST_LOG` tidak diisi.

Contoh untuk mengurangi noise SQL:

```bash
RUST_LOG=info cargo run --bin http_worker
```

Field penting dalam log meliputi `msg_id`, `tenant_id`, `logging_id`, `stream`, dan `types`. Jangan mencatat atau menyalin URL pre-signed maupun kredensial database ke sistem log eksternal tanpa kebijakan masking.

## Cache lookup dan file sementara

Cache fetcher menggunakan Moka dengan TTL 30 menit dan kapasitas maksimum 5.000 entry per
orchestrator. Key dinormalisasi menjadi `<FETCHER_UPPERCASE>:<token-trim-lowercase>`, sehingga
lookup dengan variasi spasi atau uppercase/lowercase dapat memakai entry yang sama. Cache
dibersihkan setelah job import/export selesai.

Pada IMPORT, dependency fetcher diprefetch secara batch per chunk. `import_config.fetcher_caching`
menentukan fetcher yang boleh dipakai kembali oleh dynamic dataset saat lookup lanjutan. Pada
EXPORT applicant, `export_config.fields[].is_cache = true` mengaktifkan cache untuk fetcher
internal applicant, vacancy, job grade, dan work location yang relevan.

Untuk stream IMPORT, file diunduh ke `/tmp/worker-exporter-import-<uuid>.xlsx`, dibaca sebagai
workbook, lalu dihapus setelah proses selesai. Jika muncul `relative URL without a base`, URL
hasil facilitator bukan URL absolut; periksa response `get_download_url` dan pastikan URL dapat
diakses dari container worker. Jika muncul warning cleanup, periksa permission dan kapasitas
filesystem `/tmp`.

## Operasi database

- Terapkan migrasi warehouse sebelum deployment yang memakai schema baru.
- Backup database warehouse sebelum migrasi perubahan schema yang tidak reversible secara aman.
- Semua `PgPool` dibuat saat startup, sehingga database yang tidak dipakai job tetap harus dapat diakses.
- SYNC melakukan transaksi ATS per batch. Error pada satu batch membatalkan batch tersebut; status job menjadi `SYNC_FAILED`.

## Menambah kemampuan baru

### Menambah format export

1. Implementasikan `WritterTrait` di `src/infrastructure/exporter/`.
2. Daftarkan factory baru di `build_writter_registry()` pada `src/application/writter.rs`.
3. Gunakan key tersebut pada `export_config.format`.
4. Tambahkan test writer dan uji hasil berkas per batch besar.

### Menambah dataset export

1. Implementasikan trait `Dataset` di `src/application/datasets/export/`.
2. Register di `build_dataset_registry()`.
3. Dokumentasikan kolom yang dapat dipilih pada `export_config.fields[].column`.
4. Kirim job dengan key registry pada `export_config.dataset`.

### Menambah import dataset atau loader

1. Implementasikan `ImporterDatasetV1` untuk mapper Excel baru di `src/application/datasets/import/`.
2. Register key di `build_importer_dataset_registry_v1()`.
3. Implementasikan/daftarkan `ImportLoadPersitence` bila target tabel temporary baru diperlukan.
4. Tambahkan konfigurasi mapping kolom dan contoh payload di [panduan penggunaan](ARCHITECTURE.md#source-usage).
5. Bila perlu lookup referensi, implementasikan `DataFetcherJson` dan daftarkan di `build_data_fetcher_json_registry()`.

### Menambah sync flow

1. Implementasikan `SyncDataset` untuk membentuk data master/child.
2. Implementasikan `BulkLoaderTrait` untuk tiap target tabel ATS dan daftarkan pada bulk loader registry.
3. Buat aggregator `SyncAggregator` yang memilih source, dataset, loader, serta filter sesuai `logging_id`.
4. Daftarkan aggregator di `build_sync_aggregator_registry()` dan pakai key tersebut sebagai `sync_config.sync_aggregate`.
5. Pastikan transaksi, idempotensi/upsert, dan pembaruan logging diuji sebelum job dijalankan di production.

### Menambah orchestrator baru

1. Implementasikan trait `Orchestrator` pada `src/application/orchestrator/`.
2. Daftarkan dengan key baru di `build_orchestrator_registry()`.
3. Tambahkan handler dan consumer stream yang memanggil key dengan kapitalisasi sama.
4. Tambahkan contract payload, contoh penggunaan, dan strategi error/ACK.

Poin ketiga sangat penting: registry memakai pencocokan string exact. Misalnya key `IMPORT` berbeda dari `import`.

## Batasan yang perlu diperhatikan

| Area | Kondisi saat ini | Dampak operasional |
| --- | --- | --- |
| INGESTION | Stream/group tersedia tetapi orchestrator `ingestion` tidak didaftarkan. | Jangan gunakan untuk job production. |
| Retry Redis | Pesan di-ACK setelah kegagalan handler. | Retry harus dilakukan producer atau tooling eksternal. |
| Pending claim | Tidak ada `XAUTOCLAIM`/recovery pending entry. | Buat runbook atau consumer pemulihan. |
| Stream import temporary file | File `/tmp/worker-exporter-import-<uuid>.xlsx` dihapus setelah proses. | Pastikan worker dapat menulis `/tmp`; warning cleanup perlu ditindaklanjuti. |
| Retensi export | File lokal tidak dihapus otomatis setelah upload. | Monitor ukuran `exports/` atau gunakan ephemeral storage/janitor. |
| Validasi payload | Endpoint `/test-import` meneruskan JSON tanpa validasi. | Validasi payload sebelum publish pada caller produksi. |
| Dependency startup | Lima database dan dua gRPC URL wajib. | Health service tidak akan hidup saat satu dependency unavailable. |

## Pemeriksaan sebelum deployment

- Semua environment variable tersedia dari secret manager/deployment manifest.
- Migrasi warehouse sudah diterapkan dan tervalidasi.
- Redis, seluruh database, facilitator gRPC, dan ATS gRPC dapat dijangkau dari network pod/container.
- Direktori kerja dapat menulis `exports/`.
- Consumer `stream:email` tersedia jika export harus mengirim notifikasi.
- Konfigurasi `chunk` dan jumlah replica sesuai kapasitas database.
- Dashboard/alert mencakup HTTP health, error log, Redis lag/pending, logging `FAILED`/`SYNC_FAILED`, dan pertumbuhan disk export.

---

## Source: reference-check-and-operations

# Reference check dan perubahan runtime

## Reference check export

### Architecture

```mermaid
flowchart LR
    Client[Client] -->|POST /test-import atau XADD| Redis[(stream:export)]
    Redis --> Handler[ExportHandler]
    Handler --> Orchestrator[ExportOrchestrator]
    Orchestrator --> Dataset[LsiReferenceCheckDataset]
    Dataset --> Candidate[(candidate.candidate)]
    Dataset --> Fields[(candidate.candidate_client_field)]
    Dataset --> Experience[(candidate.experient)]
    Dataset --> Writer[CSV/XLSX]
```

`lsi_reference_check_dataset` adalah key export dataset. Candidate menjadi source utama,
candidate client field menyimpan nilai reference check EAV, dan experience menjadi fallback
untuk `Last Position`, `Company Name`, serta `Position`.

Fetcher proses dan form tersedia di registry untuk consumer/dataset yang membutuhkan relasi
reference-check dan jawaban form:

| Registry key | Tabel |
| --- | --- |
| `ats_reference_check_fetcher` | `process.reference_checks` |
| `ats_reference_check_referee_fetcher` | `process.reference_check_referees` |
| `FORM` | `form.forms` |
| `FORM_QUESTION` | `form.form_questions` |
| `FORM_QUESTION_OPTION` | `form.form_question_options` |
| `FORM_RESPONSE` | `form.form_responses` |
| `FORM_RESPONSE_ANSWER` | `form.form_response_answers` |

### Penggunaan

Kirim job export melalui endpoint enqueue yang tersedia:

```bash
curl -X POST http://127.0.0.1:8080/test-import \
  -H 'content-type: application/json' \
  --data-binary @reference-check-export.json
```

### Contoh config

```json
{
  "tenant_id": "TENANT_UUID",
  "email": "requester@example.com",
  "actor_id": "ACTOR_ID",
  "orchestrator_name": "EXPORT",
  "export_config": {
    "dataset": "lsi_reference_check_dataset",
    "format": "excel",
    "fields": [
      {"name": "Check", "column": "check"},
      {"name": "Candidate ID", "column": "candidate_id"},
      {"name": "Candidate Email", "column": "candidate_email"},
      {"name": "Candidate Name", "column": "candidate_name"},
      {"name": "RR Number", "column": "rr_number"},
      {"name": "Last Position", "column": "last_position"},
      {"name": "Referral Email", "column": "referral_email"},
      {"name": "Referral Name", "column": "referral_name"},
      {"name": "Company Name", "column": "company_name"},
      {"name": "Position", "column": "position"},
      {"name": "Relation", "column": "relation"},
      {"name": "Phone Number", "column": "phone_number"},
      {"name": "Candidate Position", "column": "candidate_position"},
      {"name": "Candidate Superordinate", "column": "candidate_superordinate"},
      {"name": "Candidate Subordinate", "column": "candidate_subordinate"},
      {"name": "Candidate Join Date", "column": "candidate_join_date"},
      {"name": "Candidate End Date", "column": "candidate_end_date"},
      {"name": "Relation Period", "column": "relation_period"},
      {"name": "Candidate Responsibility", "column": "candidate_responsibility"},
      {"name": "Candidate Strength", "column": "candidate_strength"},
      {"name": "Candidate Achievement", "column": "candidate_achievement"},
      {"name": "candidate Weakness", "column": "candidate_weakness"},
      {"name": "Internal Organization", "column": "internal_organization"},
      {"name": "Internal Organization Desc", "column": "internal_organization_desc"},
      {"name": "External Organization", "column": "external_organization"},
      {"name": "External Organization Desc", "column": "external_organization_desc"},
      {"name": "Internal Case", "column": "internal_case"},
      {"name": "Internal Case Desc", "column": "internal_case_desc"},
      {"name": "External Case", "column": "external_case"},
      {"name": "External Case Desc", "column": "external_case_desc"},
      {"name": "Quit Reason", "column": "quit_reason"},
      {"name": "Current Position Recommendation", "column": "current_position_recommendation"},
      {"name": "Lead Position Recommendation", "column": "lead_position_recommendation"},
      {"name": "Collaborate with Others Recommendation", "column": "collaborate_with_others_recommendation"},
      {"name": "Recomendation Reason", "column": "recomendation_reason"},
      {"name": "Chance to Work with the Company Before", "column": "chance_to_work_with_the_company_before"},
      {"name": "Other", "column": "other"}
    ],
    "template_code": "EMAIL_TEMPLATE_CODE",
    "structure_template_code": "EMAIL_STRUCTURE_CODE",
    "redirected_url": "https://app.example.com/download?url={{url}}",
    "chunk": 500
  }
}
```

`No` tidak perlu dimasukkan ke `fields` karena ditambahkan otomatis oleh `ExportRunner`.
Key custom field camelCase dan snake_case dinormalisasi; nilai yang kosong menjadi `-`.

## Rollback REST API

### Architecture

```text
POST /rollback -> stream:rollback -> RollbackHandler -> RollbackOrchestrator -> ATS transaction
```

Endpoint hanya enqueue job. Penghapusan dilakukan asynchronous oleh worker dan logging baru
bertipe `ROLLBACK` dibuat oleh orchestrator.

### Penggunaan dan contoh config

```bash
curl -X POST http://127.0.0.1:8080/rollback \
  -H 'content-type: application/json' \
  -d '{
    "tenant_id": "TENANT_UUID",
    "email": "requester@example.com",
    "actor_id": "ACTOR_ID",
    "logging_id": "IMPORT_LOGGING_UUID",
    "pipeline": "CONFIG_ROLLBACK_PIPELINE",
    "loaders": [
      "ATS_VACANCY_REQUIREMENT_ROLLBACK",
      "ATS_VACANCY_ROLLBACK"
    ]
  }'
```

`tenant_id`, `email`, `actor_id`, `logging_id`, dan minimal satu `loaders` wajib diisi.
Loader child harus ditempatkan sebelum loader parent.

## Warehouse migration

### Architecture

```text
container start -> bootstrap() -> WAREHOUSE_MIGRATOR -> WAREHOUSE_DATABASE_URL -> http_worker
```

Migration di-embed melalui `sqlx::migrate!`. Hanya pool warehouse yang menjalankan folder
`migrations/`; ATS, organization, master, dan HRIS tidak menjalankan migration ini.

### Penggunaan

Tidak perlu `sqlx-cli` di container:

```bash
docker build -t worker-exporter .
docker run --rm --env-file .env -p 8080:8080 worker-exporter
```

SQLx mencatat versi yang sudah berhasil di `_sqlx_migrations`. Jika migration gagal, aplikasi
berhenti sebelum HTTP dan worker aktif.

---

## Source: setup-and-running

# Menjalankan aplikasi

## Prasyarat

- Rust toolchain yang kompatibel dengan edisi 2021.
- `protoc` karena `build.rs` mengompilasi dua berkas gRPC proto saat build.
- Redis yang mendukung Streams dan Consumer Groups.
- PostgreSQL yang dapat diakses untuk warehouse, organization, ATS, master, dan HRIS.
- Facilitator gRPC yang menyediakan URL unduhan serta upload file; ATS gRPC juga wajib dikonfigurasi saat bootstrap.
- Opsional: Docker untuk menjalankan image.

## Environment variable

Buat `.env` di root proyek. Jangan memasukkan kredensial nyata ke Git.

```dotenv
# HTTP; HOST dan PORT memiliki default 0.0.0.0 dan 8080.
HOST=0.0.0.0
PORT=8080

# Seluruh nilai di bawah wajib tersedia saat bootstrap.
REDIS_URL=redis://127.0.0.1:6379
WAREHOUSE_DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/warehouse
ORGANIZATION_DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/organization
ATS_DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/ats
MASTER_DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/master
HRIS_DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/hris
FACILITATOR_GRPC_URL=http://facilitator:50051
ATS_GRPC_URL=http://ats:50052
```

Catatan:

- `Config::new()` menggunakan `expect` untuk seluruh variable selain `HOST` dan `PORT`; aplikasi berhenti saat satu saja tidak ada atau koneksinya tidak dapat dibuat.
- `OLAP_DATABASE_URL` hanya dipakai oleh modul `src/config/database.rs`, bukan jalur bootstrap utama saat ini.
- Pastikan host gRPC mengikuti format URI yang diterima `tonic`, misalnya `http://nama-service:50051`.

## Menyiapkan schema warehouse

Migration pada folder `migrations/` ditujukan ke `WAREHOUSE_DATABASE_URL` dan dijalankan
otomatis oleh `bootstrap()` melalui embedded `sqlx::migrate!`. Tidak perlu meng-install
`sqlx-cli` atau `cargo-make`.

Saat menjalankan binary atau container, migration dilakukan sebelum registry dan worker dibuat:

```bash
cargo run --bin http_worker
docker build -t worker-exporter .
docker run --rm --env-file .env -p 8080:8080 worker-exporter
```

Migration yang sudah berhasil dicatat di tabel `_sqlx_migrations` dan dilewati pada startup
berikutnya. Jika migration gagal, aplikasi berhenti sebelum HTTP/worker aktif. Migration ini
hanya berjalan pada warehouse, bukan ATS atau database lainnya.

## Build dan pemeriksaan lokal

```bash
cargo check
cargo build --bin http_worker
```

Jika `protoc` belum ada, instal paket protobuf compiler dari package manager sistem terlebih dahulu. Untuk build yang menyerupai image production, fitur allocator yang dipakai Docker adalah `jemalloc`:

```bash
cargo build --release --no-default-features --features jemalloc --bin http_worker
```

## Menjalankan service

### Mode gabungan — direkomendasikan

```bash
cargo run --bin http_worker
```

Proses ini membuat HTTP server pada `HOST:PORT`, satu consumer `INGESTION`, dan masing-masing tiga consumer `IMPORT`, `EXPORT`, dan `SYNC`.

### Memisahkan HTTP dan worker

Terminal pertama:

```bash
cargo run --bin http
```

Terminal kedua:

```bash
cargo run --bin worker
```

Mode worker mandiri hanya menjalankan tiga consumer per flow IMPORT, EXPORT, dan SYNC. Jangan menyalakan `http_worker` bersamaan dengan `worker` kecuali memang ingin menambah consumer pada group Redis yang sama.

### Verifikasi startup

```bash
curl http://127.0.0.1:8080/healthz
```

Respons berhasil berbentuk:

```json
{"status":"ok","date":"2026-07-16T00:00:00+00:00"}
```

Tanggal tentu berubah mengikuti waktu server.

## Menjalankan dengan Docker

Dockerfile membangun tiga binary: `worker`, `http`, dan `http_worker`. Command default image adalah `/app/http_worker`.

```bash
docker build -t worker-exporter .
docker run --rm --env-file .env -p 8080:8080 worker-exporter
```

Image membuat `/app/exports` dan menjalankan service dengan user non-root `appuser`. Berkas export tetap berada di filesystem container; gunakan volume bila perlu melakukan inspeksi atau retensi lokal.

```bash
docker run --rm --env-file .env -p 8080:8080 \
  -v "$(pwd)/exports:/app/exports" \
  worker-exporter
```

## Menghentikan aplikasi

`worker` dan `http_worker` menangani `Ctrl-C` untuk menghentikan proses utama. Tidak ada proses graceful shutdown khusus untuk job yang sedang berjalan; saat deployment, beri waktu drain yang cukup dan periksa job yang masih `PROCESSING` sebelum mematikan instance.

---

## Source: usage

# Panduan penggunaan

## HTTP API

Base URL mengikuti `HOST` dan `PORT`, misalnya `http://127.0.0.1:8080`.

| Endpoint | Fungsi | Respons utama |
| --- | --- | --- |
| `GET /healthz` | Memeriksa HTTP process. | JSON status dan waktu UTC. |
| `POST /loggings` | Mengambil riwayat logging berdasarkan tenant. | Array record logging warehouse. |
| `POST /detail-file` | Mengambil pre-signed URL dari facilitator lalu mengarahkan client ke sana. | HTTP redirect atau 404. |
| `POST /generated-template` | Membuat template import XLSX. | File `.xlsx` sebagai attachment. |
| `POST /test-import` | Mempublikasikan body JSON apa adanya ke `stream:import`. | JSON status publish. |
| `POST /rollback` | Mengantrikan rollback ke `stream:rollback`. | JSON status `queued`. |

### Health check

```bash
curl http://127.0.0.1:8080/healthz
```

### Melihat logging tenant

```bash
curl -X POST http://127.0.0.1:8080/loggings \
  -H 'content-type: application/json' \
  -d '{"tenant_id":"TENANT_UUID"}'
```

Gunakan nilai `id` atau `stream_id` pada hasil logging saat melakukan penelusuran job. `download_url` adalah object key dari facilitator, bukan selalu URL publik penuh.

### Mengakses file export

```bash
curl -i -X POST http://127.0.0.1:8080/detail-file \
  -H 'content-type: application/json' \
  -d '{"keys":"OBJECT_KEY_DARI_DOWNLOAD_URL"}'
```

Endpoint merespons redirect menuju pre-signed URL. Tambahkan `-L` pada `curl` jika ingin mengikuti redirect dan mengunduh berkas.

### Membuat template import

```bash
curl -X POST http://127.0.0.1:8080/generated-template \
  -H 'content-type: application/json' \
  -o template.xlsx \
  -d '{
    "tenant_id": "TENANT_UUID",
    "headers": ["No. RR", "Job Position", "Headcount"],
    "example_value": [
      {"value": "RR-001", "value_type": "text", "fetcher": null, "filter": null},
      {"value": null, "value_type": "select", "fetcher": "JOB_POSITION", "filter": null},
      {"value": 2, "value_type": "text", "fetcher": null, "filter": null}
    ]
  }'
```

`tenantId`, `exampleValue`, dan `example_values` diterima sebagai alias. Nilai `value_type` yang tersedia adalah `text`, `select`, dan `multipleSelect`. Respons langsung berupa attachment XLSX, sehingga browser mengunduhnya tanpa file sementara di server. Dropdown `select` tetap memberi rekomendasi nilai, tetapi input manual tidak diblokir; lookup import teks mencocokkan nilai tanpa membedakan uppercase/lowercase. Use case ini juga membuat sheet `master_template` dengan data lookup dari sejumlah fetcher internal.

### Menguji enqueue import melalui HTTP

```bash
curl -X POST http://127.0.0.1:8080/test-import \
  -H 'content-type: application/json' \
  --data-binary @import-job.json
```

Endpoint ini tidak memvalidasi kontrak import sebelum publish. Gunakan payload valid seperti pada bagian berikut.

### Menjalankan rollback

Gunakan endpoint khusus berikut untuk memasukkan job rollback ke Redis. Endpoint tidak melakukan
delete langsung; worker menjalankan `RollbackOrchestrator` secara asynchronous.

```bash
curl -X POST http://127.0.0.1:8080/rollback \
  -H 'content-type: application/json' \
  -d '{
    "tenant_id": "TENANT_UUID",
    "email": "requester@example.com",
    "actor_id": "ACTOR_ID",
    "logging_id": "IMPORT_LOGGING_UUID",
    "loaders": ["ATS_VACANCY_REQUIREMENT_ROLLBACK", "ATS_VACANCY_ROLLBACK"]
  }'
```

`pipeline` default ke `CONFIG_ROLLBACK_PIPELINE`. `tenant_id`, `email`, `actor_id`,
`logging_id`, dan minimal satu `loaders` wajib diisi. Detail dataset reference check dan daftar
fetcher form tersedia di [reference check dan operasi](ARCHITECTURE.md#source-reference-check-and-operations).

## Kontrak Redis Streams

Setiap message harus memakai field Redis bernama **`payload`** dan nilainya adalah JSON `OrchestratorJob`.

```bash
redis-cli -u "$REDIS_URL" XADD stream:import '*' payload "$(cat import-job.json)"
```

Jangan gunakan nama field `data`, `body`, atau nama lain. Consumer hanya membaca `payload`.

Field root yang wajib dikirim:

| Field | Tipe | Keterangan |
| --- | --- | --- |
| `tenant_id` | string | Tenant pemilik job. |
| `email` | string | Penerima/notifikasi atau actor email. |
| `actor_id` | string | Identitas actor. |
| `orchestrator_name` | string | Metadata/audit. Routing aktual tetap ditentukan stream. |
| `export_config` | object/null | Wajib untuk job export. |
| `ingestion_config` | object/null | Jalur INGESTION belum didukung. |
| `import_config` | object/null | Wajib untuk job import. |
| `sync_config` | object/null | Wajib untuk job sync. |
| `filters` | array/null | Filter tambahan export. |
| `permissions_key` | array/null | Metadata izin yang dapat diteruskan ke flow sync. |

Nama field adalah `permissions_key` (jamak) pada root job, sementara di `import_config` namanya `permission_key` (tunggal).

## Menjalankan EXPORT

Kirim payload berikut ke `stream:export`. Dataset yang terdaftar saat ini mencakup
`lsi_ats_candidate_dataset`, `lsi_applicant_dataset`, dan `lsi_reference_check_dataset`; format
tersedia `csv`, `excel`, dan `excel_rs`. Lihat [reference check dan operasi](ARCHITECTURE.md#source-reference-check-and-operations)
untuk daftar header reference check.

```json
{
  "tenant_id": "TENANT_UUID",
  "email": "requester@example.com",
  "actor_id": "ACTOR_ID",
  "orchestrator_name": "EXPORT",
  "export_config": {
    "dataset": "lsi_ats_candidate_dataset",
    "format": "csv",
    "fields": [
      {"name": "Candidate ID", "column": "id"},
      {"name": "Name", "column": "name"}
    ],
    "template_code": "EMAIL_TEMPLATE_CODE",
    "structure_template_code": "EMAIL_STRUCTURE_CODE",
    "redirected_url": "https://app.example.com/download?url={{url}}",
    "chunk": 500
  },
  "ingestion_config": null,
  "import_config": null,
  "sync_config": null,
  "filters": null,
  "permissions_key": null
}
```

Contoh dengan filter tambahan:

```json
"filters": [
  {
    "field": "status",
    "operator": "=",
    "value": "ACTIVE",
    "values": null,
    "casting": {"Enum": "candidate_status_enum"}
  }
]
```

Filter tenant selalu ditambahkan oleh orchestrator. Pilih `chunk` positif yang sesuai kapasitas database dan memori. Hasil akhirnya diupload lewat facilitator; worker juga menerbitkan event email ke `stream:email`, sehingga service penerima stream tersebut harus tersedia agar notifikasi terkirim.

Dataset applicant tersedia dengan key `lsi_applicant_dataset`. Data kandidat diambil
dari `applicant_snapshot.candidate_data`, sedangkan data vacancy diambil dari vacancy
terkait. Contoh konfigurasi fields:

```json
{
  "dataset": "lsi_applicant_dataset",
  "fields": [
    { "name": "Candidate ID", "column": "id" },
    { "name": "Nama Lengkap", "column": "candidateName" },
    { "name": "No. KTP", "column": "identityNumber" },
    { "name": "Alamat Lengkap KTP", "column": "homeAddress" },
    { "name": "Provinsi KTP", "column": "addressProvince.name" },
    { "name": "Kab/Kota KTP", "column": "addressCity.name" },
    { "name": "Kecamatan KTP", "column": "addressDistrict.name" },
    { "name": "Kelurahan KTP", "column": "addressSubDistrict.name" },
    { "name": "Kode Pos KTP", "column": "addressPostalCode" },
    { "name": "Tempat Lahir", "column": "placeOfBirth.name" },
    { "name": "Tanggal Lahir", "column": "dateOfBirth" },
    { "name": "Jenis Kelamin", "column": "gender.name" },
    { "name": "Status Pernikahan", "column": "maritalStatus.name" },
    { "name": "Email", "column": "email" },
    { "name": "Nomor Telepon/HP", "column": "phoneNumber" },
    { "name": "Timestamp", "column": "createdAt" },
    { "name": "Last Update", "column": "updatedAt" },
    { "name": "RR Number", "column": null },
    { "name": "Nama Lowongan yang Dilamar (Vacancy Name)", "column": "vacancy.name" },
    { "name": "Work Location", "column": "workLocation.name", "fetcher": "MASTER_GEO", "fetcher_field_key": "id", "fetcher_key": "id", "is_cache": true },
    { "name": "Grade", "column": "jobGrade.name", "fetcher": "JOB_GRADE", "fetcher_field_key": "id", "fetcher_key": "id", "is_cache": true },
    { "name": "Job Grade", "column": "jobGrade.name", "fetcher": "JOB_GRADE", "fetcher_field_key": "id", "fetcher_key": "id", "is_cache": true },
    { "name": "Agama", "column": "religion.name" },
    { "name": "Applied Date", "column": "applicant_bucket.created_at" },
    { "name": "Nomor BPJS Ketenagakerjaan", "column": "customFields.bpjsKetenagakerjaanNumber" },
    { "name": "Nomor BPJS Kesehatan", "column": "customFields.bpjsKesehatanNumber" },
    { "name": "Nomor Rekening BCA (BNI jika Super Indo Express)", "column": "customFields.bankAccountNumber" },
    { "name": "Nomor NPWP", "column": "customFields.npwpNumber" },
    { "name": "Nama Lengkap Keluarga", "column": "contacts.0.contactName" },
    { "name": "Hubungan dengan Karyawan", "column": "contacts.0.relationship.name" },
    { "name": "Nomor Telepon", "column": "contacts.0.phone" },
    { "name": "Tanggal Bergabung (Join Date)", "column": null },
    { "name": "Ukuran Kemeja Anda (Untuk kandidat Super Indo Apprentice Program)", "column": "customFields.shirtSize.name" },
    { "name": "Ukuran Celana Formal Anda (Nomor) (Untuk kandidat Super Indo Apprentice Program)", "column": "customFields.pantsSize" },
    { "name": "Nomor ID Aplikasi My Super Indo", "column": "customFields.mySuperIndoAppIdNumber" }
  ]
}
```

Path tanpa prefix dibaca dari `candidate_data`; `customFields.*` membaca custom field
kandidat, sedangkan `vacancyCustomFields.jobGradeId` membaca custom field vacancy dengan
`field_key = jobGradeId`, kemudian ID tersebut di-resolve melalui `JOB_GRADE`. Work
location di-resolve melalui `MASTER_GEO` berdasarkan `vacancy.work_location_id`. Nilai
`null` akan diekspor sebagai `-`.

Alur resolusi `lsi_applicant_dataset` adalah:

1. `oltp_ats_applicant_bucket` menjadi source utama dan menyediakan `vacancy_id` serta cursor.
2. `ats_applicant_snapshot_fetcher` mengambil snapshot berdasarkan `applicant_bucket_id`.
3. Kandidat dibaca dari `applicant_snapshot.candidate_data`.
4. `ats_vacancy_fetcher` mengambil vacancy berdasarkan `applicant_bucket.vacancy_id`.
5. `OLTP_VACANCY_CUSTOM_FIELD` mengambil `field_key = jobGradeId`, lalu `JOB_GRADE` me-resolve ID grade.
6. `MASTER_GEO` me-resolve `vacancy.work_location_id` menjadi `workLocation`.

`is_cache: true` pada field export mengaktifkan cache untuk fetcher internal yang relevan pada
dataset applicant. Cache memakai key berbentuk `<FETCHER_UPPERCASE>:<nilai-tertrim-dan-lowercase>`
dan dibersihkan setelah job export selesai. `fetcher_field_key`, `fetcher_key`, dan
`fetcher_field_filter` tersedia sebagai kontrak umum `ExporterColumn`, tetapi mapping relasi
applicant saat ini tetap ditentukan oleh dataset; metadata tersebut belum mengganti mapping
internal secara dinamis.

## Menjalankan IMPORT

IMPORT mengunduh berkas Excel dari object key `import_config.key`; key tersebut harus dapat diterjemahkan facilitator menjadi URL absolut yang dapat diakses worker. URL relatif akan menyebabkan error `relative URL without a base`. `data_source_type: "buffer"` memilih buffer mode. Nilai selain `buffer`, misalnya `stream`, memilih stream mode.

Pada stream mode, response diunduh bertahap ke file sementara dengan pola
`/tmp/worker-exporter-import-<uuid>.xlsx`. Workbook kemudian dibaca dari file tersebut per
chunk dan file sementara dihapus setelah proses selesai, termasuk ketika proses import gagal
sejauh cleanup masih dapat dilakukan. Buffer mode membaca response di memory tanpa file
sementara. Karena itu, container worker harus mengizinkan penulisan ke `/tmp` dan log
`Download import selesai, membuka workbook dari file sementara` dapat dipakai untuk diagnosis.

Contoh job vacancy:

```json
{
  "tenant_id": "TENANT_UUID",
  "email": "requester@example.com",
  "actor_id": "ACTOR_ID",
  "orchestrator_name": "IMPORT",
  "export_config": null,
  "ingestion_config": null,
  "sync_config": {
    "sync_aggregate": "LSI_VACANCY_SYNC",
    "logging_id": null,
    "chunk": 100
  },
  "import_config": {
    "tenant_id": "TENANT_UUID",
    "group_id": "GROUP_UUID",
    "dataset": "LSI_IMPORT_VACANCY_V1",
    "auto_sync": true,
    "key": "v1/import/vacancy.xlsx",
    "data_source_type": "stream",
    "pipeline": "vacancy_import_pipeline",
    "loader": "WAREHOUSE_DYNAMIC_TEMPORARY_LOADER",
    "master_table": "vacancy",
    "primary_column": "id",
    "chunk": 500,
    "start_row": 2,
    "main_filterable": null,
    "main_searchable": null,
    "sequential_table": null,
    "fetcher_caching": null,
    "permission_key": null,
    "fields": [
      {
        "header": "No. RR",
        "column": "vacancy.filterable_attribute",
        "fetcher": null,
        "type_data": "TEXT",
        "is_custom": false,
        "fetcher_field_filter": null,
        "fetcher_field_key": null,
        "fetcher_key": null,
        "casting": null
      }
    ]
  },
  "filters": null,
  "permissions_key": null
}
```

`type_data` menerima `TEXT`, `UUID`, `DATE`, `NUMBER`, atau `Boolean`. Jika kolom memakai `fetcher`, isi key fetcher yang terdaftar dan konfigurasi `fetcher_field_filter`, `fetcher_field_key`, serta `fetcher_key` sesuai mapper dataset.

Contoh field import yang memakai fetcher:

```json
{
  "header": "Job Position",
  "column": "vacancy:position_id",
  "fetcher": "JOB_POSITION",
  "type_data": "UUID",
  "is_custom": false,
  "fetcher_field_filter": null,
  "fetcher_field_key": "id",
  "fetcher_key": "id",
  "casting": "Uuid"
}
```

`fetcher_caching` berisi daftar nama fetcher yang boleh memakai hasil lookup dari cache pada
dataset import, misalnya `"fetcher_caching": ["JOB_POSITION", "MASTER_GEO"]`. Cache lookup
menormalkan nama fetcher ke uppercase dan token lookup dengan trim/lowercase. Import melakukan
prefetch batch per chunk; jika nilai belum tersedia, dataset dynamic melakukan lookup lanjutan
per token dan mencatat cache hit, miss, serta error fetcher di log.

Dengan `auto_sync: true`, worker mengisi `sync_config.logging_id` dari logging import yang baru dibuat lalu menerbitkan job baru ke `stream:sync`. Dengan `auto_sync: false`, ambil ID logging import melalui `POST /loggings` sebelum menjalankan SYNC secara manual.

## Menjalankan SYNC

SYNC yang terdaftar saat ini adalah vacancy sync dengan aggregator `LSI_VACANCY_SYNC`. `logging_id` wajib diisi dengan ID logging import yang menghasilkan data temporary.

```json
{
  "tenant_id": "TENANT_UUID",
  "email": "requester@example.com",
  "actor_id": "ACTOR_ID",
  "orchestrator_name": "SYNC",
  "export_config": null,
  "ingestion_config": null,
  "import_config": null,
  "sync_config": {
    "sync_aggregate": "LSI_VACANCY_SYNC",
    "logging_id": "IMPORT_LOGGING_UUID",
    "chunk": 100
  },
  "filters": null,
  "permissions_key": null
}
```

Aggregator memfilter data warehouse dengan `logging_id`, lalu menjalankan bulk upsert untuk vacancy, custom field, PIC, stage, dan requirement. Status logging yang sama akan bergerak dari `SYNC_PROGRESS` menjadi `SYNC_SUCCESS` atau `SYNC_FAILED`.

## Meninjau hasil dan kesalahan

1. Cek `POST /loggings` untuk `status`, `error`, `processed_count`, `cursor`, dan `download_url`.
2. Cek log aplikasi dengan field `msg_id`, `tenant_id`, `types`, atau `logging_id`.
3. Untuk export sukses, gunakan `download_url` bersama endpoint `POST /detail-file`.
4. Untuk import/sync gagal, perbaiki input atau data target, lalu kirim job baru dengan payload yang telah diperbaiki. Pesan lama sudah di-ACK dan tidak akan dicoba ulang otomatis.

<!-- DOCS-END -->
