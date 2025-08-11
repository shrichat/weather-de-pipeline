# Weather Data Pipeline (Airflow → S3 → Postgres → Dashboard)

This is a small, end-to-end **data engineering** project.  
An Airflow job pulls current weather from OpenWeather on a schedule, saves the raw JSON to **S3**, a second job **cleans and validates** the data with Pandas and loads it into **Postgres**, and a **Dash** web app shows simple KPIs and charts. Everything runs locally with **Docker**. Secrets live in a local `.env` file (not committed publicly).

## What I built
- **Two Airflow DAGs**
  - `extract_openweather_to_s3`: calls the API, writes gzip JSON to S3 with timestamps.
  - `transform_validate_load_to_rds`: reads latest raw file, applies basic checks (ranges/schema),
    writes to Postgres with `ON CONFLICT DO NOTHING` so retries don’t duplicate rows.
- **Raw → Curated layers**
  - **S3** for raw/bronze (`raw/openweather/<CITY>/<timestamp>.json.gz`)
  - **Postgres** table `weather_observations` (primary key: `observed_at, city`)
- **Dashboard**
  - Plotly **Dash** app with KPI cards + charts, refreshing every 60s.

## Stack
Airflow, AWS S3, Postgres, Pandas, SQLAlchemy, Plotly Dash, Docker, Python 3.11

## How it works
1. Airflow calls OpenWeather and stores a timestamped gzip JSON in S3.
2. A second DAG validates/cleans the latest file and loads a single row into Postgres.
3. The Dash app queries Postgres and renders a few KPIs and charts.


| Step                   | Proof                                        |
| ---------------------- | -------------------------------------------- |
| DAGs healthy           |<img width="1429" height="499" alt="DAGS 1" src="https://github.com/user-attachments/assets/3297b102-04b0-454d-930d-0e315f47768b" />|
| Extract success        |<img width="1439" height="813" alt="DAG weather" src="https://github.com/user-attachments/assets/cd39dd42-6724-434b-89e1-5180dee12011" />|
| Transform/load success |<img width="1435" height="783" alt="DAG Transform" src="https://github.com/user-attachments/assets/22a77f11-43c0-4ee5-a0a5-d4fa0690a2ed" />|
| Raw S3 object          |<img width="1399" height="618" alt="S3 bucket 2" src="https://github.com/user-attachments/assets/64b8f231-ad5d-405e-aa9c-fb92da84d68c" />|
| Dashboard              |<img width="1140" height="653" alt="Dashboard" src="https://github.com/user-attachments/assets/ae512977-6416-4663-a87a-d7f0c7048c88" />|



