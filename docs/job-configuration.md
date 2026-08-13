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

`tenant_id`, `email`, `actor_id`, dan `orchestrator_name` tetap harus ada pada root payload.
Untuk contoh payload lengkap, lihat [panduan penggunaan](usage.md).

## `export_config`

EXPORT memilih dataset dan writer dari registry, membaca data secara cursor-based, membuat
CSV/XLSX di direktori `exports/`, mengunggah hasilnya lewat facilitator gRPC, lalu opsional
mengirim event email.

| Field | Wajib | Arti dan perilaku aktual |
| --- | --- | --- |
| `dataset` | Ya | Key export dataset. Saat ini: `lsi_ats_candidate_dataset`, `lsi_applicant_dataset`, atau alias `APPLICANT_DATASET`. Key yang tidak terdaftar membuat job gagal. |
| `format` | Ya | Writer yang dipakai: `csv` atau `excel`. `excel` menghasilkan ekstensi `.xlsx`. |
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
