from __future__ import annotations

# ขั้นวิเคราะห์แรกของ growth pipeline หลังจากข้อมูลถูก clean แล้ว
# job นี้ดึงราคาปิดแรกสุดและล่าสุดของแต่ละ ticker ออกมา

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (
    DEFAULT_CLEAN_OUTPUT,
    DEFAULT_GROWTH_START_END_OUTPUT,
    build_argument_parser,
    create_spark,
    spark_path,
)


def main() -> None:
    parser = build_argument_parser("growth_extract_start_end")
    parser.add_argument("--input-path", default=str(DEFAULT_CLEAN_OUTPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_GROWTH_START_END_OUTPUT))
    args = parser.parse_args()

    spark = create_spark(args.app_name)
    df = spark.read.parquet(spark_path(args.input_path))

    # ใช้สอง window เพื่อหาแถวแรกสุดและแถวล่าสุดของแต่ละ ticker
    first_window = Window.partitionBy("Ticker").orderBy(F.col("Date").asc())
    last_window = Window.partitionBy("Ticker").orderBy(F.col("Date").desc())

    result = (
        # เก็บเฉพาะ record ที่ใช้คำนวณจุดเริ่มต้นและจุดสิ้นสุดได้จริง
        df.filter(F.col("Ticker").isNotNull())
        .filter(F.col("Date").isNotNull())
        .filter(F.col("Close").isNotNull())
        .withColumn("start_rank", F.row_number().over(first_window))
        .withColumn("end_rank", F.row_number().over(last_window))
        .withColumn("Start_Date", F.when(F.col("start_rank") == 1, F.col("Date")))
        .withColumn("Start_Price", F.when(F.col("start_rank") == 1, F.col("Close")))
        .withColumn("End_Date", F.when(F.col("end_rank") == 1, F.col("Date")))
        .withColumn("End_Price", F.when(F.col("end_rank") == 1, F.col("Close")))
        .groupBy("Ticker")
        # หลังจากติด tag ให้แถวต้น/ปลายแล้ว ก็ aggregate กลับให้เหลือ 1 แถวต่อ 1 ticker
        .agg(
            F.max("Start_Date").alias("Start_Date"),
            F.max("Start_Price").alias("Start_Price"),
            F.max("End_Date").alias("End_Date"),
            F.max("End_Price").alias("End_Price"),
        )
    )

    result.write.mode("overwrite").parquet(spark_path(args.output_path))
    spark.stop()


if __name__ == "__main__":
    main()
