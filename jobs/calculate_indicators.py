from __future__ import annotations

# ขั้นที่ 3 ของ pipeline หลัก
# job นี้เพิ่ม technical indicators ให้กับข้อมูลรายวันของแต่ละ ticker เพื่อนำไปใช้คัดพอร์ต

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (
    DEFAULT_CLEAN_OUTPUT,
    DEFAULT_INDICATOR_OUTPUT,
    build_argument_parser,
    create_spark,
)


def main() -> None:
    parser = build_argument_parser("stock_calculate_indicators")
    parser.add_argument("--input-path", default=str(DEFAULT_CLEAN_OUTPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_INDICATOR_OUTPUT))
    args = parser.parse_args()

    spark = create_spark(args.app_name)
    df = spark.read.parquet(args.input_path)

    # rolling metrics ทุกตัวจะคำนวณแยกตาม ticker และเรียงตามเวลา
    ticker_window = Window.partitionBy("Ticker").orderBy("Date")
    ma50_window = ticker_window.rowsBetween(-49, 0)
    ma200_window = ticker_window.rowsBetween(-199, 0)
    vol30_window = ticker_window.rowsBetween(-29, 0)

    enriched = (
        # Previous_Close เป็นค่าชั่วคราวที่ใช้สำหรับคำนวณ Daily_Return
        df.withColumn("Previous_Close", F.lag("Close").over(ticker_window))
        .withColumn(
            "Daily_Return",
            F.when(
                F.col("Previous_Close").isNull(),
                F.lit(None),
            ).otherwise((F.col("Close") - F.col("Previous_Close")) / F.col("Previous_Close")),
        )
        .withColumn("MA50", F.avg("Close").over(ma50_window))
        .withColumn("MA200", F.avg("Close").over(ma200_window))
        # ส่วนเบี่ยงเบนมาตรฐาน 30 วันใช้แทนการวัด volatility แบบ rolling อย่างง่าย
        .withColumn("Volatility30", F.stddev_samp("Daily_Return").over(vol30_window))
        .drop("Previous_Close")
        .withColumn("Year", F.year("Date"))
        .withColumn("Month", F.month("Date"))
    )

    # output ของขั้นนี้คือข้อมูลตั้งต้นสำหรับการจัดอันดับและการสร้างพอร์ต
    (
        enriched.write.mode("overwrite")
        .partitionBy("Year", "Month")
        .parquet(args.output_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()
