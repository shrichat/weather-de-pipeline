# Real-Time Public Data Pipeline & Dashboard (Starter)

**Stack:** Airflow • AWS S3 • AWS RDS (Postgres) • Pandas • boto3 • Plotly Dash

## Architecture
API (OpenWeather) → Airflow (hourly) → S3 (raw) → Transform/Validate → RDS → Dash (EC2)

## Quick Start
1) Copy `.env_templates/.env.example` to `.env` and fill values.
2) `docker compose up airflow-init` then `docker compose up -d`.
3) Open Airflow at http://localhost:8080 (admin/admin). Enable both DAGs.
4) Create RDS Postgres and security group to allow your IP; run `sql/schema.sql`.
5) Run Dash locally: `pip install -r requirements.txt && python app/app.py`

## Notes
- Use AWS Budgets to cap spend; start RDS only when testing.
- Replace OpenWeather with any other API easily by editing the extract DAG.