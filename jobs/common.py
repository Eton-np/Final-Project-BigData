from __future__ import annotations

# ไฟล์รวม utility กลางที่ทุก batch job ใช้ร่วมกัน
# รวมเรื่อง path, การ parse argument, การ reset output และการสร้าง Spark session

import argparse
import importlib.util
import os
import shutil
import string
import subprocess
import sys
from datetime import datetime
from glob import glob
from pathlib import Path

_PROJECT_ROOT_ENV = os.getenv("PROJECT_ROOT")
PROJECT_ROOT = Path(_PROJECT_ROOT_ENV) if _PROJECT_ROOT_ENV else Path(__file__).resolve().parents[1]
# path เริ่มต้นทั้งหมดสามารถ override ผ่าน environment variable ได้
# ทำให้ code ชุดเดียวกันรันได้ทั้ง local, Docker Compose และ Airflow container
DEFAULT_RAW_INPUT = Path(os.getenv("RAW_INPUT_PATH", str(PROJECT_ROOT / "Data" / "StockHistory" / "*.csv")))
DEFAULT_RAW_INPUT_GROUP_BYTES = int(os.getenv("RAW_INPUT_GROUP_BYTES", str(512 * 1024 * 1024)))
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
DEFAULT_MARKET_DASHBOARD_READY_MARKER = Path(
    os.getenv(
        "MARKET_DASHBOARD_READY_MARKER_PATH",
        str(PROJECT_ROOT / "output" / "exports" / ".market_dashboard_airflow_ready"),
    )
)
DEFAULT_INVESTMENT_INSIGHTS_OUTPUT = Path(
    os.getenv(
        "INVESTMENT_INSIGHTS_OUTPUT_PATH",
        str(PROJECT_ROOT / "output" / "exports" / "investment_insights.json"),
    )
)
DEFAULT_INVESTMENT_INSIGHTS_READY_MARKER = Path(
    os.getenv(
        "INVESTMENT_INSIGHTS_READY_MARKER_PATH",
        str(PROJECT_ROOT / "output" / "exports" / ".investment_insights_airflow_ready"),
    )
)


def build_argument_parser(description: str) -> argparse.ArgumentParser:
    # ทุก job มี CLI เล็ก ๆ ของตัวเอง
    # เพื่อให้เรียกได้ทั้งแบบรันตรง, ผ่าน PowerShell script หรือผ่าน Airflow task
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--app-name", default=description.replace(" ", "_"))
    return parser


def _first_data_timestamp(path: Path) -> datetime | None:
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
            handle.readline()
            for line in handle:
                first_value = line.split(",", 1)[0].strip()
                if not first_value:
                    continue
                try:
                    return datetime.fromisoformat(first_value.replace("Z", "+00:00")).replace(tzinfo=None)
                except ValueError:
                    continue
    except OSError:
        return None
    return None


def resolve_raw_input_paths(input_path: str, max_files: int | None = None) -> list[str]:
    # ให้ความสำคัญกับไฟล์ที่มีประวัติย้อนหลังถึงปีขั้นต่ำก่อน
    # เพื่อให้ snapshot และ dashboard มีข้อมูลหลายปีย้อนหลังสม่ำเสมอกว่าเดิม
    matches = sorted(glob(input_path))
    minimum_start_year = int(os.getenv("RAW_INPUT_MIN_START_YEAR", "2015"))
    qualified: list[str] = []
    remainder: list[str] = []
    for path in matches:
        first_timestamp = _first_data_timestamp(Path(path))
        if first_timestamp is not None and first_timestamp.year <= minimum_start_year:
            qualified.append(path)
        else:
            remainder.append(path)
    matches = qualified + remainder
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


def _path_needs_windows_alias(path: Path) -> bool:
    return any(character in str(path) for character in (" ", "(", ")"))


def _existing_subst_alias(target: Path) -> Path | None:
    try:
        completed = subprocess.run(["subst"], check=False, capture_output=True, text=True)
    except OSError:
        return None
    target_text = str(target).casefold()
    for line in completed.stdout.splitlines():
        if "=>" not in line:
            continue
        drive_text, mapped_text = (part.strip() for part in line.split("=>", 1))
        if Path(mapped_text).resolve() == target or mapped_text.casefold() == target_text:
            return Path(drive_text[:2] + "\\")
    return None


def _create_subst_alias(target: Path) -> Path | None:
    existing_alias = _existing_subst_alias(target)
    if existing_alias is not None:
        return existing_alias

    import ctypes

    drive_bitmask = ctypes.windll.kernel32.GetLogicalDrives()
    used_drives = {
        string.ascii_uppercase[index]
        for index in range(len(string.ascii_uppercase))
        if drive_bitmask & (1 << index)
    }
    for drive_letter in reversed(string.ascii_uppercase):
        if drive_letter in used_drives:
            continue
        drive = f"{drive_letter}:"
        try:
            subprocess.run(["subst", drive, str(target)], check=True, capture_output=True, text=True)
        except (OSError, subprocess.CalledProcessError):
            continue
        return Path(f"{drive}\\")
    return None


def _project_alias_path(path: Path, project_alias: Path) -> Path | None:
    try:
        return project_alias / path.resolve().relative_to(PROJECT_ROOT)
    except ValueError:
        return None


def spark_path(path: str | Path) -> str:
    path_obj = Path(path)
    if os.name != "nt" or not _path_needs_windows_alias(PROJECT_ROOT):
        return str(path_obj)

    project_alias = _create_subst_alias(PROJECT_ROOT)
    if project_alias is None:
        return str(path_obj)

    alias_path = _project_alias_path(path_obj, project_alias)
    return str(alias_path or path_obj)


def _configure_windows_spark_environment() -> None:
    if os.name != "nt":
        return

    jdk17_home = Path(r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot")
    if jdk17_home.exists() and "jdk-17" not in os.getenv("JAVA_HOME", ""):
        os.environ["JAVA_HOME"] = str(jdk17_home)
        java_bin = str(jdk17_home / "bin")
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

    project_alias = None
    if _path_needs_windows_alias(PROJECT_ROOT):
        project_alias = _create_subst_alias(PROJECT_ROOT)

    hadoop_home = PROJECT_ROOT / ".hadoop" / "hadoop-3.3.6"
    if hadoop_home.exists():
        hadoop_home_alias = _project_alias_path(hadoop_home, project_alias) if project_alias else None
        resolved_hadoop_home = str(hadoop_home_alias or hadoop_home)
        os.environ.setdefault("HADOOP_HOME", resolved_hadoop_home)
        os.environ.setdefault("hadoop.home.dir", resolved_hadoop_home)
        hadoop_bin = str(Path(resolved_hadoop_home) / "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

    pyspark_spec = importlib.util.find_spec("pyspark")
    if pyspark_spec and pyspark_spec.submodule_search_locations:
        spark_home = Path(next(iter(pyspark_spec.submodule_search_locations))).resolve()
        spark_home_alias = _project_alias_path(spark_home, project_alias) if project_alias else None
        os.environ.setdefault("SPARK_HOME", str(spark_home_alias or spark_home))

    python_executable = Path(sys.executable).resolve()
    python_alias = _project_alias_path(python_executable, project_alias) if project_alias else None
    os.environ.setdefault("PYSPARK_PYTHON", str(python_alias or python_executable))
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", str(python_alias or python_executable))


def create_spark(app_name: str):
    # ฟังก์ชันกลางสำหรับสร้าง SparkSession ให้ทุก job ที่ใช้ Spark
    # config ระดับ session ช่วยให้การจัดการเวลาและการ overwrite partition เหมือนกันทั้งโปรเจกต์
    _configure_windows_spark_environment()

    from pyspark.sql import SparkSession

    master_url = os.getenv("SPARK_MASTER_URL", "local[4]")
    shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "48")
    input_group_bytes = os.getenv("RAW_INPUT_GROUP_BYTES", str(DEFAULT_RAW_INPUT_GROUP_BYTES))
    read_partition_bytes = os.getenv("SPARK_SQL_FILES_MAX_PARTITION_BYTES", str(64 * 1024 * 1024))
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.files.maxPartitionBytes", read_partition_bytes)
        .config("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize", input_group_bytes)
        .config("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", input_group_bytes)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.io.native.lib.available", "false")
    )
    if master_url:
        builder = builder.master(master_url)
    return builder.getOrCreate()
