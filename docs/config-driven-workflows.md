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
