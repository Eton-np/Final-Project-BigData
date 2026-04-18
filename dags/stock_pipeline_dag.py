from __future__ import annotations

# ไฟล์ DAG นี้เป็นจุดเริ่มต้นของการ orchestration ทั้งโปรเจกต์
# Airflow ใช้ไฟล์นี้เพื่อกำหนดว่า pipeline แต่ละตัวจะรันเมื่อไร และแต่ละ task ต้องต่อกันลำดับไหน

import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

    HAS_SPARK_PROVIDER = True
except ImportError:  # pragma: no cover - fallback for lighter Airflow installs
    from airflow.operators.bash import BashOperator

    HAS_SPARK_PROVIDER = False

if HAS_SPARK_PROVIDER:
    from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = PROJECT_ROOT / "jobs"
# ค่าพวกนี้ทำให้ DAG เดียวกันสามารถรันได้หลายสภาพแวดล้อม
# เช่น local Python, การเรียกผ่าน bash หรือผ่าน SparkSubmitOperator เมื่อมี provider ครบ
SPARK_BINARY = os.getenv("SPARK_BINARY", "spark-submit")
PYTHON_BINARY = os.getenv("PYTHON_BINARY", sys.executable or "python")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")
AIRFLOW_SPARK_TASK_MODE = os.getenv("AIRFLOW_SPARK_TASK_MODE", "bash").lower()
PYTHON_JOBS = {
    "csv_to_parquet": JOBS_DIR / "csv_to_parquet.py",
    "cleanse_data": JOBS_DIR / "cleanse_data.py",
    "calculate_indicators": JOBS_DIR / "calculate_indicators.py",
    "select_portfolio": JOBS_DIR / "select_portfolio.py",
    "export_portfolio": JOBS_DIR / "export_portfolio.py",
    "extract_growth_start_end": JOBS_DIR / "extract_growth_start_end.py",
    "calculate_growth_cagr": JOBS_DIR / "calculate_growth_cagr.py",
    "filter_rank_growth": JOBS_DIR / "filter_rank_growth.py",
    "export_growth_portfolio": JOBS_DIR / "export_growth_portfolio.py",
    "build_market_dashboard_data": JOBS_DIR / "build_market_dashboard_data.py",
    "build_investment_insights": JOBS_DIR / "build_investment_insights.py",
}
GROWTH_PARQUET_OUTPUT = PROJECT_ROOT / "output" / "parquet" / "growth_stocks"
GROWTH_CLEAN_OUTPUT = PROJECT_ROOT / "output" / "parquet" / "growth_stocks_clean"


def build_spark_task(task_id: str, script_path: Path, script_args: list[str] | None = None):
    # ฟังก์ชันกลางสำหรับสร้าง Airflow task จาก script ของโปรเจกต์แต่ละไฟล์
    # ช่วยซ่อนรายละเอียดตาม environment เพื่อให้ทุก DAG ประกาศ task ได้รูปแบบเดียวกัน
    script_args = script_args or []
    if HAS_SPARK_PROVIDER and AIRFLOW_SPARK_TASK_MODE == "spark_submit":
        return SparkSubmitOperator(
            task_id=task_id,
            application=str(script_path),
            application_args=script_args,
            conn_id="spark_default",
            conf={"spark.master": SPARK_MASTER_URL},
            verbose=False,
        )

    # โหมดสำรอง: ใช้ BashOperator เรียก script เดิมแทน
    # ทำให้โปรเจกต์ยังใช้งานได้แม้ Airflow install แบบเบาที่ไม่มี Spark provider
    runner = PYTHON_BINARY
    if AIRFLOW_SPARK_TASK_MODE == "bash_spark_submit" and shutil.which(SPARK_BINARY):
        runner = SPARK_BINARY
    quoted_args = " ".join(f'"{arg}"' for arg in script_args)
    command = f'{runner} "{script_path}"'
    if quoted_args:
        command = f"{command} {quoted_args}"
    return BashOperator(
        task_id=task_id,
        bash_command=command,
    )


with DAG(
    dag_id="stock_etl_skeleton",
    description="Skeleton DAG for the stock ETL and portfolio workflow",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 1 * *",
    catchup=False,
    tags=["stocks", "skeleton"],
) as skeleton_dag:
    # DAG นี้ตั้งใจให้เป็นโครงเปล่าเพื่อใช้โชว์ flow ใน Airflow UI
    # เหมาะสำหรับ present ลำดับ ETL โดยไม่ต้องรัน Spark job จริง
    start = EmptyOperator(task_id="start")
    ingest = EmptyOperator(task_id="ingest_csv_to_parquet")
    cleanse = EmptyOperator(task_id="cleanse_data")
    indicators = EmptyOperator(task_id="calculate_indicators")
    select = EmptyOperator(task_id="select_portfolio")
    export = EmptyOperator(task_id="export_portfolio")
    end = EmptyOperator(task_id="end")

    start >> ingest >> cleanse >> indicators >> select >> export >> end


with DAG(
    dag_id="stock_portfolio_pipeline",
    description="Monthly stock portfolio pipeline orchestrated by Airflow and Spark",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 1 * *",
    catchup=False,
    tags=["stocks", "spark", "portfolio"],
) as production_dag:
    # pipeline หลักสำหรับสร้างพอร์ตแบบรายเดือน
    # 1) ingest ไฟล์ CSV ดิบ
    # 2) cleanse ข้อมูล
    # 3) คำนวณ technical indicators
    # 4) คัดเลือกหุ้นเข้าพอร์ต
    # 5) export ออกมาเป็น CSV สำหรับใช้งานจริง
    start = EmptyOperator(task_id="start")
    ingest = build_spark_task("csv_to_parquet", PYTHON_JOBS["csv_to_parquet"])
    cleanse = build_spark_task("cleanse_data", PYTHON_JOBS["cleanse_data"])
    indicators = build_spark_task("calculate_indicators", PYTHON_JOBS["calculate_indicators"])
    select = build_spark_task("select_portfolio", PYTHON_JOBS["select_portfolio"])
    export = build_spark_task("export_portfolio", PYTHON_JOBS["export_portfolio"])
    end = EmptyOperator(task_id="end")

    start >> ingest >> cleanse >> indicators >> select >> export >> end


with DAG(
    dag_id="growth_portfolio_pipeline",
    description="Growth strategy pipeline that ranks stocks by long-term CAGR",
    start_date=datetime(2024, 1, 1),
    schedule="0 3 1 * *",
    catchup=False,
    tags=["stocks", "spark", "growth"],
) as growth_dag:
    # pipeline กลยุทธ์ growth
    # ใช้ข้อมูลประวัติชุดเดียวกัน แล้วคำนวณ CAGR ระยะยาวก่อน export หุ้นที่อันดับดีที่สุด
    start = EmptyOperator(task_id="start")
    ingest = build_spark_task(
        "csv_to_parquet",
        PYTHON_JOBS["csv_to_parquet"],
        ["--output-path", str(GROWTH_PARQUET_OUTPUT)],
    )
    cleanse = build_spark_task(
        "cleanse_data",
        PYTHON_JOBS["cleanse_data"],
        ["--input-path", str(GROWTH_PARQUET_OUTPUT), "--output-path", str(GROWTH_CLEAN_OUTPUT)],
    )
    extract = build_spark_task(
        "extract_growth_start_end",
        PYTHON_JOBS["extract_growth_start_end"],
        ["--input-path", str(GROWTH_CLEAN_OUTPUT)],
    )
    cagr = build_spark_task("calculate_growth_cagr", PYTHON_JOBS["calculate_growth_cagr"])
    rank = build_spark_task("filter_rank_growth", PYTHON_JOBS["filter_rank_growth"])
    export = build_spark_task("export_growth_portfolio", PYTHON_JOBS["export_growth_portfolio"])
    end = EmptyOperator(task_id="end")

    start >> ingest >> cleanse >> extract >> cagr >> rank >> export >> end


with DAG(
    dag_id="market_dashboard_pipeline",
    description="Prepares the market overview dataset consumed by the Market Dashboard page",
    start_date=datetime(2024, 1, 1),
    schedule="15 4 * * 1-5",
    catchup=False,
    tags=["dashboard", "market", "stocks"],
) as market_dashboard_dag:
    # DAG นี้ใช้สร้าง JSON snapshot สำหรับหน้า Market Dashboard
    # หน้า dashboard จะอ่านไฟล์ที่เตรียมไว้แล้ว แทนการคำนวณใหม่ทุกครั้งที่มี request
    start = EmptyOperator(task_id="start")
    build_market = build_spark_task("build_market_dashboard_data", PYTHON_JOBS["build_market_dashboard_data"])
    end = EmptyOperator(task_id="end")

    start >> build_market >> end


with DAG(
    dag_id="investment_insights_pipeline",
    description="Builds the score-based investment insights dataset for the second dashboard page",
    start_date=datetime(2024, 1, 1),
    schedule="30 4 * * 1-5",
    catchup=False,
    tags=["dashboard", "insights", "stocks"],
) as investment_insights_dag:
    # DAG นี้ใช้สร้าง JSON snapshot สำหรับหน้า Investment Insights
    # หน้าที่คือแปลงข้อมูลตลาดล่าสุดให้เป็นชุดหุ้นที่มีคะแนนและ label พร้อมใช้งานบนหน้าเว็บ
    start = EmptyOperator(task_id="start")
    build_insights = build_spark_task("build_investment_insights", PYTHON_JOBS["build_investment_insights"])
    end = EmptyOperator(task_id="end")

    start >> build_insights >> end
