# Weather Data Pipeline with Airflow and AWS S3

This project demonstrates a simple yet functional data pipeline built using **Apache Airflow**, running on **Docker**, to fetch real-time weather data and upload it to an **Amazon S3** bucket.

The DAG (`weather_to_s3_timestamped`) fetches current weather data for Delhi from the [Open-Meteo API](https://open-meteo.com/), stores it as a timestamped CSV, and uploads it to a partitioned folder structure in S3 (`weather/YYYY/MM/DD/`).



## Flow

- Scheduled execution (every 1 minute)
- Fetches real-time weather data via public API
- Saves timestamped CSV files locally
- Uploads files to AWS S3 with date-based folder partitioning
- Uses Airflow connections to securely handle AWS credentials
- Runs on Docker with CeleryExecutor

## Technologies Used

- Apache Airflow 2.5
- Python 3.7
- Open-Meteo API
- Amazon S3 (via Boto3)
- Docker Compose

