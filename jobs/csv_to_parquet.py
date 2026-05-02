from __future__ import annotations

# ขั้นที่ 1 ของ pipeline หลัก
# job นี้แปลงไฟล์ CSV ดิบจำนวนมากให้กลายเป็น Parquet แบบ partition เพื่อให้ขั้นถัดไปอ่านได้เร็วขึ้น

import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Iterable

from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import (
    DEFAULT_PARQUET_OUTPUT,
    DEFAULT_RAW_INPUT,
    DEFAULT_RAW_INPUT_GROUP_BYTES,
    build_argument_parser,
    create_spark,
    resolve_raw_input_paths,
    spark_path,
)


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _grouped_by_size(input_paths: list[str], group_bytes: int, max_files_per_group: int = 0) -> Iterable[list[str]]:
    current_group: list[str] = []
    current_size = 0
    for input_path in input_paths:
        path_size = Path(input_path).stat().st_size
        group_is_full = current_group and current_size + path_size > group_bytes
        group_hits_file_cap = max_files_per_group > 0 and len(current_group) >= max_files_per_group
        if group_is_full or group_hits_file_cap:
            yield current_group
            current_group = []
            current_size = 0
        current_group.append(input_path)
        current_size += path_size
    if current_group:
        yield current_group


def _last_data_timestamp(path: Path) -> datetime | None:
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            position = handle.tell()
            buffer = b""
            while position > 0:
                step = min(65536, position)
                position -= step
                handle.seek(position)
                buffer = handle.read(step) + buffer
                lines = [line for line in buffer.splitlines() if line.strip()]
                if len(lines) > 1:
                    last_line = lines[-1].decode("utf-8", errors="ignore")
                    break
            else:
                lines = [line for line in buffer.splitlines() if line.strip()]
                if len(lines) <= 1:
                    return None
                last_line = lines[-1].decode("utf-8", errors="ignore")
    except OSError:
        return None

    first_value = last_line.split(",", 1)[0].strip()
    if not first_value or first_value == "Date":
        return None
    try:
        return datetime.fromisoformat(first_value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _latest_data_timestamp(input_paths: list[str]) -> datetime | None:
    latest_date: datetime | None = None
    for input_path in input_paths:
        timestamp = _last_data_timestamp(Path(input_path))
        if timestamp is not None and (latest_date is None or timestamp > latest_date):
            latest_date = timestamp
    return latest_date


def _sanitize_columns(columns: list[str]) -> list[str]:
    sanitized_columns: list[str] = []
    for original_name in columns:
        sanitized_name = re.sub(r"[^0-9A-Za-z_]+", "_", original_name).strip("_")
        sanitized_name = sanitized_name or "column"
        candidate = sanitized_name
        suffix = 1
        while candidate in sanitized_columns:
            suffix += 1
            candidate = f"{sanitized_name}_{suffix}"
        sanitized_columns.append(candidate)
    return sanitized_columns


def _delete_source_csvs(input_paths: list[str]) -> None:
    for input_path in input_paths:
        source_path = Path(input_path).resolve()
        if source_path.suffix.lower() != ".csv":
            continue
        source_path.unlink(missing_ok=True)


def _has_parquet_files(path: Path) -> bool:
    return path.exists() and any(path.rglob("*.parquet"))


def _reset_output_path(path: Path) -> Path:
    if path.exists():
        shutil.rmtree(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _validate_parquet_output(spark, output_path: Path) -> None:
    if not _has_parquet_files(output_path):
        raise FileNotFoundError(f"No Parquet files were written to {output_path}")

    df = spark.read.parquet(spark_path(output_path))
    required_columns = {"Ticker", "Date", "Open", "High", "Low", "Close", "Volume", "Year", "Month"}
    missing_columns = sorted(required_columns.difference(df.columns))
    if missing_columns:
        raise ValueError(f"Parquet output is missing required columns: {', '.join(missing_columns)}")

    row_count = df.count()
    if row_count <= 0:
        raise ValueError("Parquet output validation failed: row count is zero")

    stats = df.agg(
        F.countDistinct("Ticker").alias("ticker_count"),
        F.min("Date").alias("min_date"),
        F.max("Date").alias("max_date"),
    ).first()
    if stats["ticker_count"] <= 0 or stats["min_date"] is None or stats["max_date"] is None:
        raise ValueError("Parquet output validation failed: ticker/date sanity checks failed")

    print(
        "Parquet validation passed: "
        f"{row_count} rows, {stats['ticker_count']} tickers, "
        f"{stats['min_date']} to {stats['max_date']}"
    )


def _prepare_batch(spark, input_paths: list[str], cutoff_timestamp: datetime | None):
    raw_df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(input_paths)
    )

    raw_df = raw_df.toDF(*_sanitize_columns(raw_df.columns))

    ticker_pattern = r"([^\\/]+)\.csv$"
    df = raw_df.withColumn("_source_file", F.input_file_name())
    # ถ้าไฟล์ต้นทางมีคอลัมน์ Ticker อยู่แล้วก็ใช้ค่านั้น
    # ถ้าไม่มี ให้ดึงชื่อหุ้นจากชื่อไฟล์ซึ่งเป็นรูปแบบการเก็บข้อมูลของโปรเจกต์นี้
    if "Ticker" in raw_df.columns:
        df = df.withColumn(
            "Ticker",
            F.when(
                F.col("Ticker").isNotNull(),
                F.col("Ticker"),
            ).otherwise(F.regexp_extract(F.col("_source_file"), ticker_pattern, 1)),
        )
    else:
        df = df.withColumn("Ticker", F.regexp_extract(F.col("_source_file"), ticker_pattern, 1))

    df = (
        df.withColumn("Date", F.to_timestamp("Date"))
        .withColumn("Year", F.year("Date"))
        .withColumn("Month", F.month("Date"))
        .drop("_source_file")
    )

    if cutoff_timestamp is not None:
        df = df.filter(F.col("Date") >= F.lit(cutoff_timestamp))

    # เก็บเฉพาะคอลัมน์หลักที่จำเป็นต่อการวิเคราะห์ในขั้นถัดไป
    base_columns = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume", "Year", "Month"]
    available_columns = [column_name for column_name in base_columns if column_name in df.columns]
    df = df.select(*available_columns)
    numeric_casts = {
        "Open": T.DoubleType(),
        "High": T.DoubleType(),
        "Low": T.DoubleType(),
        "Close": T.DoubleType(),
        "Volume": T.DoubleType(),
    }
    for column_name, data_type in numeric_casts.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, F.col(column_name).cast(data_type))

    # ตัด record ที่ข้อมูลสำคัญไม่ครบออก ก่อนเขียนเป็น Parquet สำหรับการวิเคราะห์
    required_columns = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]
    available_required_columns = [column_name for column_name in required_columns if column_name in df.columns]
    df = df.dropna(subset=available_required_columns)
    df = df.filter(F.col("Date").isNotNull())
    return df.repartition("Year", "Month")


def main() -> None:
    parser = build_argument_parser("stock_csv_to_parquet")
    parser.add_argument("--input-path", default=str(DEFAULT_RAW_INPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_PARQUET_OUTPUT))
    parser.add_argument("--max-files", type=int, default=int(os.getenv("RAW_INPUT_MAX_FILES", "0")))
    parser.add_argument("--years-back", type=int, default=int(os.getenv("RAW_INPUT_YEARS_BACK", "10")))
    parser.add_argument(
        "--group-bytes",
        type=int,
        default=DEFAULT_RAW_INPUT_GROUP_BYTES,
        help="Approximate total source CSV bytes to process per batch. Defaults to 512MB.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("RAW_INPUT_BATCH_SIZE", "0")),
        help="Optional maximum number of source files per 512MB group. Use 0 for no file-count cap.",
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        default=_env_flag("PARQUET_FORCE_REBUILD"),
        help="Rebuild Parquet even when existing Parquet files are already present.",
    )
    parser.add_argument(
        "--delete-source-csv",
        action="store_true",
        default=_env_flag("RAW_INPUT_DELETE_SOURCE_CSV"),
        help="Delete source CSV files only after the full Parquet output passes validation.",
    )
    parser.add_argument(
        "--no-reset-output",
        action="store_true",
        default=_env_flag("PARQUET_NO_RESET_OUTPUT"),
        help="Append into the output path instead of clearing it before the run.",
    )
    args = parser.parse_args()

    if args.group_bytes <= 0:
        raise ValueError("--group-bytes must be greater than zero")
    if args.batch_size < 0:
        raise ValueError("--batch-size cannot be negative")

    output_path = Path(args.output_path)
    if _has_parquet_files(output_path) and not args.force_rebuild:
        print(f"Parquet output already exists at {output_path}; skipping conversion. Use --force-rebuild to refresh it.")
        return

    input_paths = resolve_raw_input_paths(args.input_path, args.max_files)
    if not input_paths:
        raise FileNotFoundError(f"No CSV files matched input path: {args.input_path}")

    spark = create_spark(args.app_name)

    # จำกัดช่วงเวลาย้อนหลังจากวันที่ล่าสุดใน dataset
    # ใช้วันที่ในข้อมูลแทน current_timestamp เพื่อให้ dataset historical ยังตัดช่วงได้คงที่แม้รันปีถัดไป
    cutoff_timestamp = None
    if args.delete_source_csv and args.years_back > 0:
        print("--delete-source-csv is enabled; keeping all historical rows and ignoring --years-back")
    elif args.years_back > 0:
        latest_date = _latest_data_timestamp(input_paths)
        if latest_date is not None:
            cutoff_timestamp = spark.range(1).select(
                F.add_months(F.lit(latest_date), -(args.years_back * 12)).alias("cutoff")
            ).first()["cutoff"]

    progress_marker = output_path.parent / f".{output_path.name}_csv_to_parquet_in_progress"
    should_reset_output = args.force_rebuild or (not args.no_reset_output and not progress_marker.exists())
    if should_reset_output:
        output_path = _reset_output_path(output_path)
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.delete_source_csv:
        progress_marker.write_text("in_progress\n", encoding="utf-8")

    batch_groups = list(_grouped_by_size(input_paths, args.group_bytes, args.batch_size))
    total_batches = len(batch_groups)
    for batch_number, batch_paths in enumerate(batch_groups, start=1):
        batch_bytes = sum(Path(batch_path).stat().st_size for batch_path in batch_paths)
        print(
            f"Converting group {batch_number}/{total_batches}: "
            f"{len(batch_paths)} CSV files, {batch_bytes / 1024 / 1024:.2f} MB"
        )
        df = _prepare_batch(spark, batch_paths, cutoff_timestamp)

        # partition ตาม Year/Month เพื่อให้การอ่านข้อมูลตามช่วงเวลาของ job ถัดไปเร็วขึ้น
        (
            df.write.mode("append")
            .partitionBy("Year", "Month")
            .parquet(spark_path(output_path))
        )

    _validate_parquet_output(spark, output_path)

    if args.delete_source_csv:
        _delete_source_csvs(input_paths)
        print(f"Deleted {len(input_paths)} source CSV files after successful Parquet validation")

    progress_marker.unlink(missing_ok=True)

    spark.stop()


if __name__ == "__main__":
    main()
