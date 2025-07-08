# Batch ETL Pipeline with PySpark & Airflow

## 📂 Dataset
- `point_distance.csv`: Jarak antar bandara
- `monthly_flights.csv`: Jumlah penerbangan per bulan
- `edu_avg_flights.csv`: Rata-rata level pendidikan di sekitar bandara

## ⚙️ Arsitektur Pipeline
1. Airflow DAG dijadwalkan harian (`@daily`)
2. Menjalankan PySpark ETL
3. Menyimpan hasil ke PostgreSQL (melalui JDBC atau `COPY`)

## 🔄 Alur DAG
- `create_table`: Membuat tabel jika belum ada
- `run_etl_pyspark`: Proses ETL dengan PySpark
- `load_to_postgres`: Menyimpan hasil ke DB


