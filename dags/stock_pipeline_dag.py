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
# ถ้าไม่มี provider ของ Spark ติดตั้งอยู่ ก็จะ fallback ไปใช้ BashOperator แทน
    HAS_SPARK_PROVIDER = True
except ImportError:  # pragma: no cover - fallback for lighter Airflow installs
    from airflow.operators.bash import BashOperator

    HAS_SPARK_PROVIDER = False

if HAS_SPARK_PROVIDER:
    from airflow.operators.bash import BashOperator

# ค่าพวกนี้ทำให้ DAG เดียวกันสามารถรันได้หลายสภาพแวดล้อม
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
# พาธพวกนี้ใช้ร่วมกันระหว่างหลาย DAG เพื่อให้แน่ใจว่าแต่ละ pipeline จะใช้ข้อมูลชุดเดียวกัน และลดการซ้ำซ้อนของการสร้างไฟล์ intermediate
RAW_PARQUET_OUTPUT = Path(os.getenv("PARQUET_OUTPUT_PATH", str(PROJECT_ROOT / "output" / "parquet" / "stocks")))
SHARED_CLEAN_OUTPUT = Path(os.getenv("CLEAN_OUTPUT_PATH", str(PROJECT_ROOT / "output" / "parquet" / "stocks_clean")))


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

# ฟังก์ชันนี้ช่วยให้ DAG สามารถตรวจสอบได้ว่าข้อมูลที่ผ่านการ clean แล้วมีอยู่หรือไม่ก่อนที่จะรัน task ที่ต้องใช้ข้อมูลนั้น
def build_clean_data_if_missing_task(task_id: str, input_path: Path, output_path: Path):
    command = f"""
    set -e
    if [ -d "{output_path}" ] && find "{output_path}" -type f -name '*.parquet' -print -quit | grep -q .; then
      echo "Reusing existing cleaned dataset at {output_path}"
    else
      echo "Cleaned dataset is missing; building {output_path}"
      {PYTHON_BINARY} "{PYTHON_JOBS["cleanse_data"]}" --input-path "{input_path}" --output-path "{output_path}"
    fi
    """
    return BashOperator(task_id=task_id, bash_command=command)

#Dagนี้เอาไว้สำหรับรัน pipeline หลักที่สร้างพอร์ตหุ้นรายเดือน 
with DAG(
    dag_id="stock_portfolio_pipeline",
    description="Monthly stock portfolio pipeline orchestrated by Airflow and Spark",
    start_date=datetime(2024, 1, 1),
    schedule=None,
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

# DAG นี้เป็น pipeline สำหรับกลยุทธ์ growth ที่เน้นการคัดเลือกหุ้นที่มี CAGR ระยะยาวดีที่สุด
with DAG(
    dag_id="growth_portfolio_pipeline",
    description="Growth strategy pipeline that ranks stocks by long-term CAGR",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["stocks", "spark", "growth"],
) as growth_dag:
    # pipeline กลยุทธ์ growth
    # 1.) ingest ไฟล์ CSV ดิบ (ใช้ task เดียวกับ production_dag เพื่อให้แน่ใจว่าใช้ข้อมูลชุดเดียวกัน)
    # 2.) ตรวจสอบว่าข้อมูลที่ผ่านการ clean แล้วมีอยู่หรือไม่ ถ้าไม่มีให้สร้างขึ้นมาใหม่ (เพื่อให้แน่ใจว่า pipeline นี้สามารถรันได้แม้ไม่เคยรัน pipeline หลักมาก่อน)
    # 3.) ดึงข้อมูลราคาหุ้นในอดีตมาคำนวณจุดเริ่มต้นและจุดสิ้นสุดของช่วงเวลาที่จะคำนวณ CAGR
    # 4.) คำนวณ CAGR ของแต่ละหุ้น
    # 5.) คัดเลือกหุ้นที่มี CAGR สูงสุดมาเป็นพอร์ตโฟลิโอ
    # 6.) export ออกมาเป็น CSV สำหรับใช้งานจริง
    start = EmptyOperator(task_id="start")
    ensure_parquet = build_spark_task("ensure_stockhistory_parquet", PYTHON_JOBS["csv_to_parquet"])
    ensure_clean = build_clean_data_if_missing_task("ensure_clean_data", RAW_PARQUET_OUTPUT, SHARED_CLEAN_OUTPUT)
    extract = build_spark_task(
        "extract_growth_start_end",
        PYTHON_JOBS["extract_growth_start_end"],
        ["--input-path", str(SHARED_CLEAN_OUTPUT)],
    )
    cagr = build_spark_task("calculate_growth_cagr", PYTHON_JOBS["calculate_growth_cagr"])
    rank = build_spark_task("filter_rank_growth", PYTHON_JOBS["filter_rank_growth"])
    export = build_spark_task("export_growth_portfolio", PYTHON_JOBS["export_growth_portfolio"])
    end = EmptyOperator(task_id="end")

    start >> ensure_parquet >> ensure_clean >> extract >> cagr >> rank >> export >> end

# DAG นี้เป็น pipeline สำหรับ refresh snapshot ที่หน้าเว็บอ่านใช้งาน เช่น dashboard และ insights ต่างๆ
with DAG(
    dag_id="dashboard_refresh_pipeline",
    description="Refreshes the prepared Market Dashboard and Investment Insights snapshots",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["stocks", "dashboard", "web"],
) as dashboard_dag:
    #pipeline สำหรับ refresh snapshot ที่หน้าเว็บอ่านใช้งาน เช่น dashboard และ insights ต่างๆ
    #1.) ตรวจสอบว่าข้อมูลที่ผ่านการ clean แล้วมีอยู่หรือไม่ ถ้าไม่มีให้สร้างขึ้นมาใหม่ (เพื่อให้แน่ใจว่า pipeline นี้สามารถรันได้แม้ไม่เคยรัน pipeline หลักมาก่อน)
    #2.) สร้าง dataset สำหรับหน้า dashboard ซึ่งจะถูกนำไปใช้แสดงผลในหน้าเว็บ และ API ที่เกี่ยวข้อง
    #3.) สร้าง dataset สำหรับหน้า investment insights ซึ่งจะถูกนำไปใช้แสดงผลในหน้าเว็บ และ API ที่เกี่ยวข้อง
    start = EmptyOperator(task_id="start")
    ensure_parquet = build_spark_task("ensure_stockhistory_parquet", PYTHON_JOBS["csv_to_parquet"])
    build_market = build_spark_task(
        "build_market_dashboard_data",
        PYTHON_JOBS["build_market_dashboard_data"],
        ["--mark-airflow-run"],
    )
    build_insights = build_spark_task(
        "build_investment_insights",
        PYTHON_JOBS["build_investment_insights"],
        ["--mark-airflow-run"],
    )
    end = EmptyOperator(task_id="end")

    start >> ensure_parquet >> build_market >> build_insights >> end
