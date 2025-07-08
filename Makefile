.PHONY: build init up down spark-run airflow-shell

# Build Docker images
build:
	docker-compose build

# Inisialisasi Airflow DB & buat user admin
init:
	docker-compose run airflow-webserver airflow db init
	docker-compose run airflow-webserver airflow users create \
	  --username admin --password admin \
	  --firstname Admin --lastname User --role Admin --email admin@111.com

# Jalankan semua service
up:
	docker-compose up -d

# Hentikan semua service
down:
	docker-compose down

# Jalankan script PySpark secara manual (debug)
spark-run:
	docker-compose exec spark spark-submit /scripts/etl_pyspark.py

# Masuk ke shell Airflow Webserver
airflow-shell:
	docker-compose exec airflow-webserver bash
