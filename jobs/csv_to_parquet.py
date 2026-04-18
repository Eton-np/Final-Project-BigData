from __future__ import annotations

# ขั้นที่ 1 ของ pipeline หลัก
# job นี้แปลงไฟล์ CSV ดิบจำนวนมากให้กลายเป็น Parquet แบบ partition เพื่อให้ขั้นถัดไปอ่านได้เร็วขึ้น

import os
import re

from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import (
    DEFAULT_PARQUET_OUTPUT,
    DEFAULT_RAW_INPUT,
    build_argument_parser,
    create_spark,
    reset_output_path,
    resolve_raw_input_paths,
)


def main() -> None:
    parser = build_argument_parser("stock_csv_to_parquet")
    parser.add_argument("--input-path", default=str(DEFAULT_RAW_INPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_PARQUET_OUTPUT))
    parser.add_argument("--max-files", type=int, default=int(os.getenv("RAW_INPUT_MAX_FILES", "100")))
    parser.add_argument("--years-back", type=int, default=int(os.getenv("RAW_INPUT_YEARS_BACK", "8")))
    args = parser.parse_args()

    spark = create_spark(args.app_name)
    input_paths = resolve_raw_input_paths(args.input_path, args.max_files)
    if not input_paths:
        raise FileNotFoundError(f"No CSV files matched input path: {args.input_path}")

    # อ่าน CSV ทุกไฟล์ที่ match เข้ามารวมเป็น Spark DataFrame เดียว
    # ปิด schema inference ไว้เพื่อเลี่ยงปัญหาไฟล์ดิบที่รูปแบบไม่สม่ำเสมอ
    raw_df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(input_paths)
    )

    # ทำชื่อคอลัมน์ให้เป็นมาตรฐานเพื่อให้ job ถัดไปอ้างอิงได้ปลอดภัย
    # เช่นตัด space/อักขระพิเศษ และกันชื่อซ้ำหลัง sanitize
    sanitized_columns: list[str] = []
    for original_name in raw_df.columns:
        sanitized_name = re.sub(r"[^0-9A-Za-z_]+", "_", original_name).strip("_")
        sanitized_name = sanitized_name or "column"
        candidate = sanitized_name
        suffix = 1
        while candidate in sanitized_columns:
            suffix += 1
            candidate = f"{sanitized_name}_{suffix}"
        sanitized_columns.append(candidate)

    raw_df = raw_df.toDF(*sanitized_columns)

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

    # จำกัดช่วงเวลาย้อนหลังตามที่กำหนด เพื่อให้การรัน local/Airflow จัดการข้อมูลขนาดใหญ่ได้ง่ายขึ้น
    if args.years_back > 0:
        df = df.filter(F.col("Date") >= F.add_months(F.current_timestamp(), -(args.years_back * 12)))

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
    df = df.repartition("Year", "Month")
    output_path = reset_output_path(args.output_path)

    # partition ตาม Year/Month เพื่อให้การอ่านข้อมูลตามช่วงเวลาของ job ถัดไปเร็วขึ้น
    (
        df.write.mode("overwrite")
        .partitionBy("Year", "Month")
        .parquet(str(output_path))
    )

    spark.stop()


if __name__ == "__main__":
    main()
