from __future__ import annotations

# ขั้น export ของกลยุทธ์ growth
# ทำหน้าที่แปลงผลจัดอันดับจาก Parquet ให้เป็น CSV ไฟล์เดียว โดยเก็บเฉพาะ top-N

import shutil
from pathlib import Path

from common import (
    DEFAULT_GROWTH_EXPORT_OUTPUT,
    DEFAULT_GROWTH_RANKED_OUTPUT,
    build_argument_parser,
    create_spark,
    spark_path,
)


def main() -> None:
    parser = build_argument_parser("growth_export_portfolio")
    parser.add_argument("--input-path", default=str(DEFAULT_GROWTH_RANKED_OUTPUT))
    parser.add_argument("--output-file", default=str(DEFAULT_GROWTH_EXPORT_OUTPUT))
    parser.add_argument("--top-n", type=int, default=20)
    args = parser.parse_args()

    spark = create_spark(args.app_name)
    # จำกัด top-N ที่ขั้นนี้ เพื่อให้ Parquet ต้นทางยังเก็บ universe ที่ถูกจัดอันดับครบทั้งชุดได้
    df = spark.read.parquet(spark_path(args.input_path)).limit(args.top_n)

    output_file = Path(args.output_file).resolve()
    temp_dir = output_file.parent / f".{output_file.stem}_spark_tmp"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if output_file.exists():
        output_file.unlink()

    # ใช้แนวทางเดียวกับ export พอร์ตหลัก: Spark เขียนเป็นโฟลเดอร์ แล้วค่อยย้ายไฟล์ part เดียวมาเป็นไฟล์จริง
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(spark_path(temp_dir))

    part_file = next(temp_dir.glob("part-*.csv"))
    shutil.move(str(part_file), str(output_file))
    shutil.rmtree(temp_dir)

    spark.stop()


if __name__ == "__main__":
    main()
