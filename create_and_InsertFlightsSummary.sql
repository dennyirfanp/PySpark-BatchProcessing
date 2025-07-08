CREATE TABLE IF NOT EXISTS flights_summary (
    Airport TEXT,
    Year INT,
    Total_Flights INT,
    Edu_Level FLOAT
);

TRUNCATE TABLE flights_summary;

COPY flights_summary(Airport, Year, Total_Flights, Edu_Level)
FROM '/opt/airflow/output/cleaned_data/part-00000.csv'
DELIMITER ','
CSV HEADER;
