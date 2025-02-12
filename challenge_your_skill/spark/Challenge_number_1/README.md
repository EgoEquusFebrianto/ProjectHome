# ETL Sederhana

## Deskripsi Proyek
Proyek ini bertujuan untuk mengekstrak, mentransformasi, dan memuat (ETL) data transaksi dari sumber eksternal ke dalam sistem manajemen dan penyimpanan data.

## Data Source
download dataset dan masukkan kedalam repository github anda.

tampilkan linknya disini:
[transaction.csv](https://github.com/EgoEquusFebrianto/data_resource/blob/main/spark/transaction.csv)

## Tantangan
1. **Ekstraksi:**
   - Rancang program yang dapat mengambil data dari URL yang disediakan secara online.
2. **Transformasi:**
   - Lakukan transformasi pada data sesuai dengan dokumen `docs.txt`.
3. **Pemuatan:**
   - Muat hasil transformasi ke dalam sistem manajemen dan penyimpanan data. Pilih salah satu dari:
     - PostgreSQL
     - MySQL
     - Google BigQuery
     - Hive
     - Delta Lake
     - MongoDB
     - Kafka
     - Cassandra
   - **Catatan:** Penyimpanan dalam **HDFS adalah wajib** sebelum data dimuat ke sistem lain.

## Teknologi yang Digunakan
- Apache Spark untuk pemrosesan data
- HDFS untuk penyimpanan sementara
- Sistem database pilihan untuk penyimpanan akhir

## Cara Menjalankan
1. **Persiapan Lingkungan:**
   - Pastikan Apache Spark dan HDFS telah dikonfigurasi dengan benar.
   - Instal dependensi yang diperlukan.

2. **Eksekusi Program:**
   Jalankan skrip ETL untuk mengambil, mengolah, dan memuat data:
   ```bash
   spark-submit etl_script.py
   ```

3. **Verifikasi Data:**
   - Periksa HDFS untuk memastikan data telah tersimpan.
   - Cek database tujuan untuk memastikan data berhasil dimuat.

## Struktur Proyek
```
ETL_Sederhana/
│── scripts/             # Folder berisi skrip Fungsi maupun Pengaturan Spark
│── main.py              # applikasi spark
│── README.md            # Dokumen ini
```

## Tambahan
Struktur proyek diatas boleh ditambah, namun harus dipastikan struktur tersebut ada dan sesuai. Pastikan anda mengerjakan proyek ini dengan sungguh-sungguh SENDIRIAN. Anda boleh melewatkan tantangan ini karna ini hanyalah kasus pembelajaran. Jika anda berminat, kerjakan dan Post hasilnya di Site General. Team akan menganalisa dan mengirimkan komentar hasil pekerjaan Anda.

## Lisensi
Proyek ini dibuat untuk keperluan pembelajaran dan terbuka untuk dikembangkan lebih lanjut.

---
