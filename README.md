# Stock Portfolio Pipeline

End-to-end Apache Airflow + PySpark project for ingesting large US stock CSV files, converting them to partitioned Parquet, calculating financial indicators, generating a monthly portfolio, and showing the latest portfolio on a live web dashboard.

## What This Project Does

1. Ingests raw stock CSV files from `Data/StockHistory/*.csv`
2. Converts them into Parquet partitioned by `Year` and `Month`
3. Cleans nulls and filters out rows with no trading activity
4. Computes `Daily_Return`, `MA50`, `MA200`, and `Volatility30`
5. Selects up to 20 stocks per month using the rule `MA50 > MA200` and lowest volatility
6. Exports the final monthly portfolio into `output/exports/final_portfolio.csv`
7. Serves a web dashboard that merges the latest portfolio with the latest rows from `Data/Todays_stocks.csv`
8. Builds JSON snapshots for a filterable `Market Dashboard` and `Investment Insights` page

## Project Structure

```text
.
|-- dags/
|   `-- stock_pipeline_dag.py
|-- jobs/
|   |-- common.py
|   |-- csv_to_parquet.py
|   |-- cleanse_data.py
|   |-- calculate_indicators.py
|   |-- select_portfolio.py
|   |-- export_portfolio.py
|   |-- dashboard_datasets.py
|   |-- build_market_dashboard_data.py
|   `-- build_investment_insights.py
|-- scripts/
|   |-- run_dashboard.ps1
|   `-- run_local_pipeline.ps1
|-- web/
|   |-- app.py
|   |-- portfolio_service.py
|   |-- static/
|   `-- templates/
|-- tests/
|   |-- test_dashboard_payload.py
|   `-- test_project_structure.py
|-- Data/
|-- docker-compose.yml
|-- requirements.txt
`-- .env.example
```

## Airflow DAGs

- `stock_etl_skeleton`
  Purpose: show the complete task flow in Airflow UI with empty tasks only
- `stock_portfolio_pipeline`
  Purpose: run the real Spark jobs in sequence from ingestion to final CSV export
- `growth_portfolio_pipeline`
  Purpose: run the growth strategy flow from clean stock history to `Top20_Growth_Portfolio.csv`
- `market_dashboard_pipeline`
  Purpose: build `output/exports/market_dashboard.json` for the Market Dashboard page
- `investment_insights_pipeline`
  Purpose: build `output/exports/investment_insights.json` for the Investment Insights page

## Web Dashboard

The final business-facing output is now a pair of interactive web pages, not just CSV files.

- Page route: `/`
- Secondary page route: `/insights`
- JSON API route: `/api/portfolio`
- Secondary JSON API route: `/api/investment-insights`
- Prepared source 1: `output/exports/market_dashboard.json`
- Prepared source 2: `output/exports/investment_insights.json`
- Underlying source files: `Data/Todays_stocks.csv`, `output/exports/final_portfolio.csv`, `output/exports/Top20_Growth_Portfolio.csv`

The Market Dashboard shows:

- Filterable market breadth summary
- Performance heatmap
- Risk-vs-return scatter view
- Movers, activity, and signal mix
- Universe explorer table

The Investment Insights page shows:

- Score-based labels such as `Worth Watching`, `Stable`, and `Caution`
- Spotlight names with reasons and warnings
- Score distribution and ranking ladder
- Overlap with the existing portfolio and growth watchlist
- A ranked insight explorer table

## Local Run With Spark

Install dependencies in your Python environment, then run:

```powershell
spark-submit jobs/csv_to_parquet.py
spark-submit jobs/cleanse_data.py
spark-submit jobs/calculate_indicators.py
spark-submit jobs/select_portfolio.py
spark-submit jobs/export_portfolio.py
python jobs/build_market_dashboard_data.py
python jobs/build_investment_insights.py
```

Or use the helper script:

```powershell
.\scripts\run_local_pipeline.ps1
```

## Run The Dashboard

After generating the required exports and JSON snapshots, you can start the dashboard in either of these ways:

Run locally with Python:

```powershell
python run_dashboard.py
```

Optional live-reload for local development:

```powershell
$env:DASHBOARD_RELOAD="1"
python run_dashboard.py
```

Or run with Docker:

```powershell
.\scripts\run_dashboard.ps1
```

To keep it in the background:

```powershell
.\scripts\run_dashboard.ps1 -Detached
```

Then open:

- Dashboard: `http://localhost:8000`

Important:

- Do not open [index.html](c:/Users/napho/Downloads/archive%20(3)/web/templates/index.html) directly with `file:///...`
- This file is a Jinja template, so it must be rendered by FastAPI first
- If you open it directly in the browser, you will see raw `{{ ... }}` and `{% ... %}` template code
- Avoid running `python run_dashboard.py` in parallel with Docker, because it can create confusing host/port conflicts
- On some Windows setups, auto-reload can fail with a permission error; if that happens, leave `DASHBOARD_RELOAD` unset and run without reload

## Run With Docker Compose

This repo includes a minimal Airflow stack with Postgres:
```powershell
docker compose up airflow-init
docker compose up
```

Then open:

- Airflow UI: `http://localhost:8082`
- Username: `airflow`
- Password: `airflow`

Airflow in this project is intended to run through Docker Compose only.

## Configurable Environment Variables

The pipeline reads these environment variables when available:

- `PROJECT_ROOT`
- `RAW_INPUT_PATH`
- `RAW_INPUT_MAX_FILES`
- `RAW_INPUT_YEARS_BACK`
- `PARQUET_OUTPUT_PATH`
- `CLEAN_OUTPUT_PATH`
- `INDICATOR_OUTPUT_PATH`
- `PORTFOLIO_OUTPUT_PATH`
- `EXPORT_OUTPUT_PATH`
- `SPARK_MASTER_URL`
- `SPARK_SQL_SHUFFLE_PARTITIONS`
- `SPARK_BINARY`
- `AIRFLOW_SPARK_TASK_MODE`
- `PORTFOLIO_SIZE`
- `PORTFOLIO_CAPITAL_BASE`

Copy `.env.example` to `.env` and adjust values if needed.

Recommended default:

- `AIRFLOW_SPARK_TASK_MODE=bash`
  This works without creating an Airflow `spark_default` connection.
- Switch to `AIRFLOW_SPARK_TASK_MODE=spark_submit` only when your Airflow environment already has the Spark provider and a configured `spark_default` connection.
- For faster local/Airflow runs on very large datasets, start with `RAW_INPUT_MAX_FILES=100` and `RAW_INPUT_YEARS_BACK=8`.

## Output Locations

- Raw-to-Parquet: `output/parquet/stocks`
- Cleaned data: `output/parquet/stocks_clean`
- Indicators: `output/parquet/stocks_indicators`
- Portfolio parquet: `output/parquet/portfolio`
- Final export: `output/exports/final_portfolio.csv`
- Growth start/end parquet: `output/parquet/growth_start_end`
- Growth CAGR parquet: `output/parquet/growth_cagr`
- Growth ranked parquet: `output/parquet/growth_ranked`
- Growth export: `output/exports/Top20_Growth_Portfolio.csv`
- Market dashboard snapshot: `output/exports/market_dashboard.json`
- Investment insights snapshot: `output/exports/investment_insights.json`

## Tests

Run:

```powershell
pytest
```

## Notes

- The current dataset already stores one CSV per ticker, so the ingestion job automatically derives `Ticker` from the source filename when the column is missing.
- The dashboard is "real time" relative to the latest available local file data. If `Data/Todays_stocks.csv` is updated, the page reflects it on the next refresh cycle.
- The code is structured so the same jobs can be triggered manually with `spark-submit` or orchestrated through Airflow.
