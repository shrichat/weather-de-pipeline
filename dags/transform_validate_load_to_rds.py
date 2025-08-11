from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, io, json, gzip
import boto3, pandas as pd
from sqlalchemy import create_engine, text

AWS_REGION = os.getenv("AWS_DEFAULT_REGION","us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")

RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = os.getenv("RDS_PORT","5432")
RDS_DB = os.getenv("RDS_DB")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")
RDS_SSLMODE = os.getenv("RDS_SSLMODE","require")

def _engine():
    conn = f"postgresql+psycopg2://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DB}?sslmode={RDS_SSLMODE}"
    return create_engine(conn)

def list_latest_raw_key(prefix):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if "Contents" not in resp: 
        raise RuntimeError("No raw files found")
    keys = sorted([o["Key"] for o in resp["Contents"] if o["Key"].endswith(".json.gz")])
    return keys[-1]

def transform_validate_load(**context):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    city = os.getenv("CITY","Chicago,US").replace(",","_")
    key = list_latest_raw_key(f"raw/openweather/{city}/")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    by = io.BytesIO(obj["Body"].read())
    with gzip.GzipFile(fileobj=by, mode="rb") as gz:
        payload = json.loads(gz.read().decode("utf-8"))
    dt = payload["payload"]
    row = {
        "observed_at": pd.to_datetime(dt["dt"], unit="s", utc=True),
        "city": dt["name"],
        "temp_c": dt["main"]["temp"],
        "humidity": dt["main"]["humidity"],
        "wind_speed": dt["wind"]["speed"],
        "weather_desc": dt["weather"][0]["description"] if dt.get("weather") else None
    }
    df = pd.DataFrame([row])
    assert df["temp_c"].between(-80, 80).all(), "Temp out of range"
    assert df["humidity"].between(0,100).all(), "Humidity out of range"
    eng = _engine()
    with eng.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS weather_observations (
            observed_at TIMESTAMPTZ NOT NULL,
            city TEXT NOT NULL,
            temp_c NUMERIC,
            humidity NUMERIC,
            wind_speed NUMERIC,
            weather_desc TEXT,
            PRIMARY KEY (observed_at, city)
        );
        """))
    rows = df.to_dict(orient="records")
    with eng.begin() as conn:
        conn.execute(text("""
            INSERT INTO weather_observations
                (observed_at, city, temp_c, humidity, wind_speed, weather_desc)
            VALUES
                (:observed_at, :city, :temp_c, :humidity, :wind_speed, :weather_desc)
            ON CONFLICT (observed_at, city) DO NOTHING
        """), rows)
    return key

with DAG(
    dag_id="transform_validate_load_to_rds",
    schedule_interval="15 * * * *",
    start_date=datetime(2025, 8, 10),
    catchup=False,
    default_args={"owner":"shri","retries":2,"retry_delay": timedelta(minutes=5)},
    tags=["transform","validate","load","rds"],
) as dag:
    t = PythonOperator(
        task_id="transform_validate_load",
        python_callable=transform_validate_load,
        provide_context=True,
    )
    t