# Implementasi Medallion Architecture dengan Apache Spark untuk Pengelolaan dan Pengelompokan Data Transaksi E-Commerce

## Deskripsi  
Project ini mengimplementasikan pipeline data menggunakan **Medallion Architecture** untuk pengelompokan produk berdasarkan data transaksi e-commerce wilayah Lampung/Sumatera. Pipeline dibangun menggunakan **Apache Spark** yang berjalan di dalam **Docker container** berbasis Ubuntu 24.04.

## Anggota Kelompok  
- YOGY SA`E TAMA — 121450041  
- ALVIA ASRINDA BR.GINTING — 122450077  
- IRHAMNA MAHDI — 122450049  
- Danang Hilal Kurniawan — 122450085  
- Daffa Ahmad Naufal — 122450137
  
## Arsitektur Medallion  
- **Bronze Layer**: Menyimpan data mentah transaksi produk dalam format CSV.  
- **Silver Layer**: Data sudah dibersihkan, fitur teks diekstraksi menggunakan TF-IDF dan PCA, serta data siap untuk analisis.  
- **Gold Layer**: Hasil clustering produk menggunakan algoritma KMeans, disimpan dalam format Parquet dan CSV.

## Teknologi  
- Apache Spark 3.5.5  
- Docker (Ubuntu 24.04)  
- Python 3 dan PySpark MLlib  
- Dataset CSV transaksi dan review produk

## Cara Penggunaan

1. **Build dan jalankan Docker container:**  
   ```bash
   bash build.sh
   bash start.sh
   ```
2. **Copy dataset CSV ke container:**
   ```bash
   docker cp C:/bigdata-ecommerce/data/product_reviews_dirty.csv bigdata-ecommerce:/data/product_reviews_dirty.csv
   ```
3. **Login ke container:**
   ```bash
   bash login.sh
   ```
4. **Jalankan pipeline Medallion Architecture:**
   ```bash
   spark-submit /pipeline_medallion.py /data/product_reviews_dirty.csv /output
   ```
5. **Hasil pipeline akan tersimpan di dalam container:**
   - `/output/silver` dan `/output/gold` (format Parquet)
   - `/output/silver_csv` dan `/output/gold_csv` (format CSV)
   ```bash
   bash login.sh
   ```
6. **Copy hasil CSV ke host Windows untuk analisis dan visualisasi:**
   ```bash
    docker cp bigdata-ecommerce:/output/silver_csv C:/bigdata-ecommerce/output/silver_csv
    docker cp bigdata-ecommerce:/output/gold_csv C:/bigdata-ecommerce/output/gold_csv
   ```

## Struktur Folder Project
`pipeline_medallion.py` — Script pipeline utama

`export_gold_to_csv.py` — (Opsional) Script ekspor hasil Gold ke CSV

`Dockerfile`, `build.sh`, `start.sh`, `login.sh`, `stop.sh` — Konfigurasi dan skrip Docker

`data/` — Folder dataset CSV
