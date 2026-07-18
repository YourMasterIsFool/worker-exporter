# Dokumentasi Worker Exporter

`worker-exporter` adalah layanan Rust berbasis Tokio yang menjalankan pekerjaan asinkron melalui Redis Streams. Implementasi yang siap dipakai saat ini mencakup proses **IMPORT**, **EXPORT**, dan **SYNC**, serta HTTP API untuk health check, logging, file download, dan pembuatan template Excel.

Dokumentasi ini ditulis dari implementasi kode saat ini. Nama registry, payload, dan perilaku operasional di bawah adalah kontrak aktual aplikasi, bukan rancangan target.

## Mulai dari sini

1. [Menjalankan aplikasi](setup-and-running.md) — prasyarat, environment variable, migrasi, dan pilihan executable.
2. [Panduan penggunaan](usage.md) — endpoint HTTP, payload Redis, serta contoh IMPORT, EXPORT, dan SYNC.
3. [Arsitektur](architecture.md) — alur runtime, layer, registry, dan integrasi eksternal.
4. [Struktur direktori](directory-structure.md) — fungsi tiap folder dan berkas penting.
5. [Operasional dan pengembangan](operations-and-development.md) — observabilitas, delivery Redis, batasan, dan cara memperluas fitur.

## Pilihan executable

| Perintah | Fungsi | Kapan dipakai |
| --- | --- | --- |
| `cargo run --bin http_worker` | Menjalankan HTTP API dan worker dalam satu proses. | Pilihan lokal dan container default. |
| `cargo run --bin worker` | Menjalankan tiga worker Redis: IMPORT, EXPORT, dan SYNC. | Deployment worker terpisah. |
| `cargo run --bin http` | Menjalankan HTTP API tanpa worker. | Deployment API terpisah. |
| `cargo run --bin worker-exporter` | Memanggil bootstrap lalu berhenti setelah sekitar 10 detik. | Bukan entry point service untuk production. |

`http_worker` membuat satu worker `INGESTION` dan tiga worker untuk tiap flow EXPORT, IMPORT, serta SYNC. Namun orchestrator `INGESTION` belum didaftarkan; karena itu flow tersebut belum dapat digunakan end-to-end. Detailnya ada di [arsitektur](architecture.md#status-implementasi-dan-batasan).

## Ringkasan kemampuan

| Flow | Stream | Status | Hasil utama |
| --- | --- | --- | --- |
| IMPORT | `stream:import` | Aktif | File Excel dipetakan dan dimuat ke tabel temporary warehouse. |
| EXPORT | `stream:export` | Aktif | CSV/XLSX dibuat, diunggah lewat facilitator, lalu event email dipublikasikan. |
| SYNC | `stream:sync` | Aktif | Data temporary vacancy diproses dan di-upsert ke ATS OLTP. |
| INGESTION | `stream:ingestion` | Belum siap | Consumer group dibuat, tetapi orchestrator tidak tersedia. |

## Dokumen lama

`ID_ARCHITECTUE_MD.md`, `EN_ARCHITECTURE_MD.md`, dan `ARCHICTECTURE.MD` adalah dokumen lama yang tetap dipertahankan agar tautan historis tidak putus. Gunakan rangkaian dokumen ini sebagai referensi operasional utama.
