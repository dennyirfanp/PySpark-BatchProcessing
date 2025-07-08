from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Buat Spark session
spark = SparkSession.builder \
    .appName("BatchETL") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# Load dataset
point_df = spark.read.csv("/opt/airflow/data/point_distance.csv", header=True, inferSchema=True)
monthly_df = spark.read.csv("/opt/airflow/data/monthly_flights.csv", header=True, inferSchema=True)
edu_df = spark.read.csv("/opt/airflow/data/edu_avg_flights.csv", header=True, inferSchema=True)

# Transformasi: join dan filter
result_df = monthly_df.join(edu_df, on='Airport', how='inner') \
                      .withColumnRenamed('Avg_Education_Level', 'Edu_Level') \
                      .filter(col("Total_Flights") > 1000)

# Simpan ke PostgreSQL (opsional jika tidak lewat COPY)
result_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/airflow") \
    .option("dbtable", "flights_summary") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Atau alternatif simpan ke file
# result_df.write.mode('overwrite').option("header", True).csv("/opt/airflow/output/cleaned_data")
