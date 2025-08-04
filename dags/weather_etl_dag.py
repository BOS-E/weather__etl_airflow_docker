from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import boto3
import pandas as pd
import requests

def fetch_and_upload():
    # Get current time for file naming
    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d_%H-%M-%S")

    # Fetch weather data
    url = "https://api.open-meteo.com/v1/forecast?latitude=28.61&longitude=77.23&current_weather=true"
    data = requests.get(url).json()['current_weather']
    df = pd.DataFrame([data])

    # Save to local file
    file_path = f"/tmp/weather_data_{timestamp_str}.csv"
    df.to_csv(file_path, index=False)

    # Get AWS creds from Airflow connection
    conn = BaseHook.get_connection("aws_creds")  # use your custom conn ID
    aws_access_key = conn.login
    aws_secret_key = conn.password

    # Upload to S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    folder_prefix = now.strftime("weather/%Y/%m/%d")
    file_name = f"weather_data_{now.strftime('%H-%M-%S')}.csv"
    s3_path = f"{folder_prefix}/{file_name}"

    s3.upload_file(file_path, "airflowbuckett1000", s3_path)
    print("Uploaded:", file_path)

# Airflow DAG
with DAG(
    dag_id="weather_to_s3_timestamped",
    start_date=datetime(2023, 1, 1),
    schedule_interval="* * * * *",  # every 1 minute
    catchup=False,
) as dag:
    
    upload_task = PythonOperator(
        task_id="fetch_and_upload",
        python_callable=fetch_and_upload,
    )
