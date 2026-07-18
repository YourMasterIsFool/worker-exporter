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

1. Implementasikan trait `Dataset` di `src/infrastructure/datasets/export/`.
2. Register di `build_dataset_registry()`.
3. Dokumentasikan kolom yang dapat dipilih pada `export_config.fields[].column`.
4. Kirim job dengan key registry pada `export_config.dataset`.

### Menambah import dataset atau loader

1. Implementasikan `ImporterDatasetV1` untuk mapper Excel baru di `src/infrastructure/datasets/import/`.
2. Register key di `build_importer_dataset_registry_v1()`.
3. Implementasikan/daftarkan `ImportLoadPersitence` bila target tabel temporary baru diperlukan.
4. Tambahkan konfigurasi mapping kolom dan contoh payload di [panduan penggunaan](usage.md).
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
