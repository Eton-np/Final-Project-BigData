from __future__ import annotations

# ไฟล์รวม utility กลางที่ทุก batch job ใช้ร่วมกัน
# รวมเรื่อง path, การ parse argument, การ reset output และการสร้าง Spark session

import argparse
import os
import shutil
from glob import glob
from pathlib import Path

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1])).resolve()
# path เริ่มต้นทั้งหมดสามารถ override ผ่าน environment variable ได้
# ทำให้ code ชุดเดียวกันรันได้ทั้ง local, Docker Compose และ Airflow container
DEFAULT_RAW_INPUT = Path(os.getenv("RAW_INPUT_PATH", str(PROJECT_ROOT / "Data" / "StockHistory" / "*.csv")))
DEFAULT_PARQUET_OUTPUT = Path(os.getenv("PARQUET_OUTPUT_PATH", str(PROJECT_ROOT / "output" / "parquet" / "stocks")))
DEFAULT_CLEAN_OUTPUT = Path(os.getenv("CLEAN_OUTPUT_PATH", str(PROJECT_ROOT / "output" / "parquet" / "stocks_clean")))
DEFAULT_INDICATOR_OUTPUT = Path(
    os.getenv("INDICATOR_OUTPUT_PATH", str(PROJECT_ROOT / "output" / "parquet" / "stocks_indicators"))
)
DEFAULT_PORTFOLIO_OUTPUT = Path(
    os.getenv("PORTFOLIO_OUTPUT_PATH", str(PROJECT_ROOT / "output" / "parquet" / "portfolio"))
)
DEFAULT_EXPORT_OUTPUT = Path(
    os.getenv("EXPORT_OUTPUT_PATH", str(PROJECT_ROOT / "output" / "exports" / "final_portfolio.csv"))
)
DEFAULT_GROWTH_START_END_OUTPUT = Path(
    os.getenv("GROWTH_START_END_OUTPUT_PATH", str(PROJECT_ROOT / "output" / "parquet" / "growth_start_end"))
)
DEFAULT_GROWTH_CAGR_OUTPUT = Path(
    os.getenv("GROWTH_CAGR_OUTPUT_PATH", str(PROJECT_ROOT / "output" / "parquet" / "growth_cagr"))
)
DEFAULT_GROWTH_RANKED_OUTPUT = Path(
    os.getenv("GROWTH_RANKED_OUTPUT_PATH", str(PROJECT_ROOT / "output" / "parquet" / "growth_ranked"))
)
DEFAULT_GROWTH_EXPORT_OUTPUT = Path(
    os.getenv("GROWTH_EXPORT_OUTPUT_PATH", str(PROJECT_ROOT / "output" / "exports" / "Top20_Growth_Portfolio.csv"))
)
DEFAULT_MARKET_DASHBOARD_OUTPUT = Path(
    os.getenv("MARKET_DASHBOARD_OUTPUT_PATH", str(PROJECT_ROOT / "output" / "exports" / "market_dashboard.json"))
)
DEFAULT_INVESTMENT_INSIGHTS_OUTPUT = Path(
    os.getenv(
        "INVESTMENT_INSIGHTS_OUTPUT_PATH",
        str(PROJECT_ROOT / "output" / "exports" / "investment_insights.json"),
    )
)


def build_argument_parser(description: str) -> argparse.ArgumentParser:
    # ทุก job มี CLI เล็ก ๆ ของตัวเอง
    # เพื่อให้เรียกได้ทั้งแบบรันตรง, ผ่าน PowerShell script หรือผ่าน Airflow task
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--app-name", default=description.replace(" ", "_"))
    return parser


def resolve_raw_input_paths(input_path: str, max_files: int | None = None) -> list[str]:
    # เรียงไฟล์ดิบเพื่อให้ลำดับการประมวลผลคงที่
    # การเรียงตามขนาดก่อนช่วยให้ไฟล์เล็กถูกหยิบไปประมวลผลก่อนในหลายกรณี
    matches = sorted(glob(input_path), key=lambda path: (Path(path).stat().st_size, path))
    if max_files is not None and max_files > 0:
        matches = matches[:max_files]
    return matches


def reset_output_path(path: str | Path) -> Path:
    # ส่วนใหญ่ job จะเขียน output ใหม่ทั้งชุด
    # การล้าง path เดิมก่อนช่วยกันไม่ให้ไฟล์เก่าปะปนกับผลรอบใหม่
    output_path = Path(path).resolve()
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path


def create_spark(app_name: str):
    # ฟังก์ชันกลางสำหรับสร้าง SparkSession ให้ทุก job ที่ใช้ Spark
    # config ระดับ session ช่วยให้การจัดการเวลาและการ overwrite partition เหมือนกันทั้งโปรเจกต์
    from pyspark.sql import SparkSession

    master_url = os.getenv("SPARK_MASTER_URL")
    shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "48")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
    )
    if master_url:
        builder = builder.master(master_url)
    return builder.getOrCreate()
