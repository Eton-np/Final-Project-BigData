from __future__ import annotations

# ขั้นสุดท้ายของ pipeline พอร์ตหลัก
# job นี้แปลงผลลัพธ์ Parquet จาก Spark ให้เป็น CSV ไฟล์เดียวที่ dashboard และผู้ใช้ธุรกิจอ่านง่าย

import csv
import shutil
from pathlib import Path

from common import (
    DEFAULT_EXPORT_OUTPUT,
    DEFAULT_PORTFOLIO_OUTPUT,
    build_argument_parser,
    create_spark,
    spark_path,
)


EXPORT_COLUMNS = [
    "Date",
    "Year",
    "Month",
    "Ticker",
    "Close",
    "MA50",
    "MA200",
    "Volatility30",
    "selection_rank",
    "Weight",
]


def write_empty_export(output_file: Path) -> None:
    # ถ้าไม่มีข้อมูลพอร์ต ก็ยังเขียน CSV เปล่าที่มี header ไว้
    # เพื่อให้ระบบปลายทางไม่พังจากการหาไฟล์ไม่เจอหรือ schema หาย
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(EXPORT_COLUMNS)


def main() -> None:
    parser = build_argument_parser("stock_export_portfolio")
    parser.add_argument("--input-path", default=str(DEFAULT_PORTFOLIO_OUTPUT))
    parser.add_argument("--output-file", default=str(DEFAULT_EXPORT_OUTPUT))
    args = parser.parse_args()

    output_file = Path(args.output_file).resolve()
    temp_dir = output_file.parent / f".{output_file.stem}_spark_tmp"
    # ล้างไฟล์และโฟลเดอร์ชั่วคราวของรอบก่อน เพื่อให้ทุกรอบได้ export ใหม่แบบสะอาด
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if output_file.exists():
        output_file.unlink()

    input_path = Path(args.input_path).resolve()
    parquet_files = list(input_path.rglob("*.parquet")) if input_path.exists() else []
    if not parquet_files:
        write_empty_export(output_file)
        return

    spark = create_spark(args.app_name)
    # เรียงลำดับข้อมูลก่อน export เพื่อให้ไฟล์ที่ได้อ่านง่ายและผลลัพธ์คงที่
    df = spark.read.parquet(spark_path(args.input_path)).orderBy("Year", "Month", "selection_rank", "Ticker")

    # Spark เขียน CSV ออกมาเป็นโฟลเดอร์
    # จึงใช้ coalesce(1) เพื่อบังคับให้เหลือไฟล์ part เดียว แล้วค่อย rename เป็นไฟล์ปลายทางจริง
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(spark_path(temp_dir))

    part_file = next(temp_dir.glob("part-*.csv"))
    shutil.move(str(part_file), str(output_file))
    shutil.rmtree(temp_dir)

    spark.stop()


if __name__ == "__main__":
    main()
