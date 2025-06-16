# PySpark-BatchProcessing
Detail Assignment Batch Processing with PySpark

## Proyek: *Monthly Flights ETL Automation with PySpark & Airflow*

## Alur Kerja DAG Airflow

DAG `monthly_flights_etl` dijalankan secara otomatis dengan alur berikut:

1. **Start** – Menandai dimulainya proses ETL.
2. **run_etl** – Menjalankan skrip PySpark menggunakan `spark-submit`, yang berisi proses ETL.
3. **End** – Menandai akhir eksekusi DAG.

DAG dijadwalkan untuk berjalan **setiap hari** (`@daily`) dan tidak melakukan catchup untuk eksekusi yang terlewat.

##Proses ETL (Extract - Transform - Load)

### Extract
- Membaca file CSV bernama `monthly_flights.csv`.
- File berisi data jumlah penerbangan bulanan, dengan kolom: `date`, `monthly_total_flights`.

### Transform
- Agregasi dilakukan untuk menghitung jumlah penerbangan per tanggal:

  ```sql
  SELECT date, SUM(monthly_total_flights) AS total_flights
  FROM monthly_flights
  GROUP BY date
  ```

- Hasilnya disimpan dalam DataFrame dengan dua kolom: `date` dan `total_flights`.

### Load
- Output hasil transformasi disimpan ke:
  - ✅ **PostgreSQL** → Tabel: `public.monthly_flights_summary`
  - ✅ **File CSV tunggal** → `/usr/local/airflow/output/monthly_flights_summary.csv`
- Hasil juga ditampilkan ke **console log Airflow** menggunakan `df.show()`.

---

##Analisis Batch yang Dilakukan

Transformasi dilakukan untuk menjawab pertanyaan bisnis berikut:
- **Berapa total penerbangan per tanggal?**
- Dapat digunakan untuk:
  - Memonitor tren volume penerbangan
  - Menemukan musim puncak dan tren penurunan
  - Membantu perencanaan logistik, bandara, atau layanan maskapai

---
