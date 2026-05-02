# Stock Portfolio Pipeline

End-to-end Apache Airflow + PySpark project for building a stock analytics pipeline from raw CSV files to partitioned Parquet, portfolio outputs, and an interactive FastAPI dashboard.

## What This Project Does

1. Ingests raw stock CSV files from `Data/StockHistory/*.csv`
2. Converts the raw CSV files into Parquet partitioned by `Year` and `Month`
3. Cleans invalid rows, fills missing price values where possible, removes rows with no trading activity, and deduplicates by `Ticker` and `Date`
4. Calculates technical indicators including `Daily_Return`, `MA50`, `MA200`, `Volatility30`, `RSI_14`, `MACD_12_26_9`, and `CCI_14_0_015`
5. Selects a monthly core portfolio using `MA50 > MA200`, lowest volatility, and equal weighting
6. Builds a growth watchlist ranked by long-term CAGR
7. Writes Parquet datasets, CSV exports, and dashboard JSON snapshots
8. Serves a web dashboard for market overview, investment insights, and architecture explanation

The current Docker/Airflow default uses `PORTFOLIO_SIZE=10`, so the core portfolio export contains up to 10 stocks per month. Change `PORTFOLIO_SIZE` in `.env.example` or `docker-compose.yml` if you want a different portfolio size.

## Repository Scope

This repository is intended to store the project code and documentation only. Large raw datasets and generated outputs are intentionally ignored by Git.

Ignored local data/output paths:

- `Data/`
- `Stock_List.csv`
- `output/`
- `.venv-airflow/`
- `airflow_home_local/`
- `.hadoop/`
- `architecture-check*.png`

Before running the pipeline, place the source CSV files locally under:

```text
Data/StockHistory/*.csv
```

The pipeline derives `Ticker` from each CSV filename when the source file does not already include a `Ticker` column.

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
|   |-- extract_growth_start_end.py
|   |-- calculate_growth_cagr.py
|   |-- filter_rank_growth.py
|   |-- export_growth_portfolio.py
|   |-- dashboard_datasets.py
|   |-- dashboard_snapshot_spark.py
|   |-- build_market_dashboard_data.py
|   `-- build_investment_insights.py
|-- notebooks/
|   `-- stockhistory_eda.ipynb
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
|-- docker-compose.yml
|-- Dockerfile.airflow
|-- requirements.txt
|-- run_dashboard.py
|-- .env.example
`-- README.md
```

## Pipeline Flow

Main portfolio pipeline:

```text
CSV ingest -> Parquet -> clean -> indicators -> portfolio selection -> CSV export
```

Growth pipeline:

```text
clean stock history -> start/end extraction -> CAGR calculation -> growth ranking -> CSV export
```

Dashboard refresh:

```text
Parquet datasets -> market_dashboard.json -> investment_insights.json -> FastAPI/Jinja dashboard
```

## Airflow DAGs

- `stock_portfolio_pipeline`: runs the main Spark jobs from raw CSV ingestion to `final_portfolio.csv`
- `growth_portfolio_pipeline`: builds the Top 20 growth portfolio from long-term CAGR
- `dashboard_refresh_pipeline`: refreshes `market_dashboard.json` and `investment_insights.json`

The DAGs are manual-trigger DAGs. The dashboard shows data after a local pipeline run or Airflow DAG run creates the prepared snapshot files.

## Web Dashboard

Routes:

- `/`: Market Dashboard
- `/insights`: Investment Insights
- `/growth`: alias for Investment Insights
- `/architecture`: pipeline architecture page for presentation
- `/api/portfolio`: market dashboard JSON
- `/api/investment-insights`: investment insights JSON
- `/api/growth-portfolio`: legacy alias for investment insights JSON

The Market Dashboard includes:

- Market breadth summary
- Year selector based on first-to-last trading day in the selected year
- Filters for ticker, signal, RSI zone, price band, and portfolio/growth membership
- Performance heatmap
- Risk-vs-return scatter chart
- Movers, activity, signal mix, and universe table

The Investment Insights page includes:

- Score-based labels: `Worth Watching`, `Stable`, and `Caution`
- Spotlight candidates with reasons and warnings
- Score distribution and ranking ladder
- Portfolio/growth watchlist overlap
- Ranked candidate table

## Local Run

Install dependencies first:

```powershell
pip install -r requirements.txt
```

Run the full local pipeline:

```powershell
.\scripts\run_local_pipeline.ps1
```

Or run each step manually:

```powershell
spark-submit jobs/csv_to_parquet.py
spark-submit jobs/cleanse_data.py
spark-submit jobs/calculate_indicators.py
spark-submit jobs/select_portfolio.py
spark-submit jobs/export_portfolio.py
python jobs/build_market_dashboard_data.py --mark-airflow-run
python jobs/build_investment_insights.py --mark-airflow-run
```

For a full 10-year rebuild from CSV:

```powershell
spark-submit jobs/csv_to_parquet.py --force-rebuild --years-back 10 --max-files 0 --group-bytes 536870912
```

After Parquet exists, `jobs/csv_to_parquet.py` skips conversion unless `--force-rebuild` is passed.

## Run The Dashboard

After the JSON snapshots exist, run:

```powershell
python run_dashboard.py
```

Then open:

```text
http://localhost:8000
```

Optional live reload:

```powershell
$env:DASHBOARD_RELOAD="1"
python run_dashboard.py
```

Do not open `web/templates/index.html` directly with `file:///...`; it is a Jinja template and must be rendered through FastAPI.

## Docker Compose

Start Airflow and the dashboard with Docker Compose:

```powershell
docker compose up airflow-init
docker compose up
```

Then open:

- Airflow UI: `http://localhost:8082`
- Dashboard: `http://localhost:8000`
- Username: `airflow`
- Password: `airflow`

Airflow in this project is intended to run through Docker Compose.

## Environment Variables

Important defaults:

- `RAW_INPUT_PATH=Data/StockHistory/*.csv`
- `RAW_INPUT_MAX_FILES=0`
- `RAW_INPUT_YEARS_BACK=10`
- `RAW_INPUT_GROUP_BYTES=536870912`
- `RAW_INPUT_MIN_START_YEAR=2015`
- `DASHBOARD_DATASET_ENGINE=spark`
- `DASHBOARD_ALLOW_CSV_FALLBACK=0`
- `DASHBOARD_HISTORY_MAX_TICKERS=0`
- `SPARK_MASTER_URL=local[4]`
- `SPARK_SQL_SHUFFLE_PARTITIONS=48`
- `AIRFLOW_SPARK_TASK_MODE=bash`
- `PORTFOLIO_SIZE=10` in Docker Compose
- `GROWTH_MINIMUM_YEARS=1`
- `GROWTH_MINIMUM_PRICE=5`

Copy `.env.example` to `.env` if you want local overrides.

## Output Locations

- Raw Parquet: `output/parquet/stocks`
- Cleaned Parquet: `output/parquet/stocks_clean`
- Indicator Parquet: `output/parquet/stocks_indicators`
- Portfolio Parquet: `output/parquet/portfolio`
- Final portfolio CSV: `output/exports/final_portfolio.csv`
- Growth start/end Parquet: `output/parquet/growth_start_end`
- Growth CAGR Parquet: `output/parquet/growth_cagr`
- Growth ranked Parquet: `output/parquet/growth_ranked`
- Growth CSV export: `output/exports/Top20_Growth_Portfolio.csv`
- Market dashboard snapshot: `output/exports/market_dashboard.json`
- Investment insights snapshot: `output/exports/investment_insights.json`

These outputs are generated artifacts and are not committed to Git.

## Tests

Run:

```powershell
pytest
```

On this Windows workspace, the bundled environment can be used with:

```powershell
.\.venv-airflow\Scripts\python.exe -m pytest -q
```

## Presentation Notes

For oral presentation, explain the project in this order:

1. Raw CSV data is too large and slow to scan repeatedly, so the project converts it to partitioned Parquet.
2. The clean step makes the stock history usable by filtering invalid trading rows and standardizing date/year/month fields.
3. The analysis layer calculates moving averages, volatility, RSI, MACD, CCI, monthly portfolio selection, and growth CAGR.
4. Airflow orchestrates the jobs, while FastAPI serves prepared dashboard snapshots.
5. The dashboard is current relative to the latest generated snapshot, not live market pricing.

## Notes

- The project uses Spark/Parquet by default for dashboard dataset generation.
- CSV fallback code exists for debugging, but it is disabled by default because direct CSV scans are slow for this dataset size.
- The dashboard refresh should run after the stock or growth pipeline finishes if you want the web pages to show the newest generated results.
