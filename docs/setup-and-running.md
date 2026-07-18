# Menjalankan aplikasi

## Prasyarat

- Rust toolchain yang kompatibel dengan edisi 2021.
- `protoc` karena `build.rs` mengompilasi dua berkas gRPC proto saat build.
- Redis yang mendukung Streams dan Consumer Groups.
- PostgreSQL yang dapat diakses untuk warehouse, organization, ATS, master, dan HRIS.
- Facilitator gRPC yang menyediakan URL unduhan serta upload file; ATS gRPC juga wajib dikonfigurasi saat bootstrap.
- Opsional: Docker untuk menjalankan image, `cargo-make` untuk task migrasi.

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

Migrasi pada folder `migrations/` ditujukan ke `WAREHOUSE_DATABASE_URL`.

Jika `cargo-make` tersedia:

```bash
cargo make migrate-main
```

Alternatif dengan SQLx CLI:

```bash
cargo sqlx migrate run --database-url "$WAREHOUSE_DATABASE_URL" --ignore-missing
```

Task berikut tersedia di `Makefile.toml`:

| Perintah | Efek |
| --- | --- |
| `cargo make migrate-main` | Menerapkan seluruh migrasi warehouse. |
| `cargo make migrate-info` | Menampilkan status migrasi. |
| `cargo make migrate-main-revert` | Membalik migrasi terakhir. |
| `cargo make db-reset` | Menghapus, membuat ulang, lalu memigrasikan database warehouse. Bersifat destruktif. |

Jalankan `db-reset` hanya pada database lokal/non-produksi yang memang boleh dihapus.

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
