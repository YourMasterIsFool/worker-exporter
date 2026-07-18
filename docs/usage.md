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

Kirim payload berikut ke `stream:export`. Dataset yang terdaftar saat ini adalah `lsi_ats_candidate_dataset`; format tersedia `csv` dan `excel`.

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

## Menjalankan IMPORT

IMPORT mengunduh berkas Excel dari object key `import_config.key`; key tersebut harus dapat diterjemahkan facilitator menjadi URL yang dapat diakses worker. `data_source_type: "buffer"` memilih buffer mode. Nilai selain `buffer`, misalnya `stream`, memilih stream mode.

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
