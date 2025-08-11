from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, json, requests, io, gzip
import boto3

AWS_REGION = os.getenv("AWS_DEFAULT_REGION","us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")
API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = os.getenv("CITY","Chicago,US")

def fetch_and_upload(**context):
    assert S3_BUCKET, "S3_BUCKET env not set"
    assert API_KEY, "OPENWEATHER_API_KEY not set"
    ts = context['ts']
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    key = f"raw/openweather/{CITY.replace(',','_')}/{ts}.json.gz"
    by = json.dumps({"fetched_at": ts, "payload": data}).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(by)
    buf.seek(0)
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue(), ContentType="application/json", ContentEncoding="gzip")
    return key

with DAG(
    dag_id="extract_openweather_to_s3",
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 8, 10),
    catchup=False,
    default_args={"owner":"shri","retries":2,"retry_delay": timedelta(minutes=5)},
    tags=["extract","openweather","s3"],
) as dag:
    t1 = PythonOperator(
        task_id="fetch_and_upload_raw",
        python_callable=fetch_and_upload,
        provide_context=True,
    )
    t1