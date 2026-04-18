from __future__ import annotations

# ขั้นที่ 4 ของ pipeline พอร์ตหลัก
# job นี้นำข้อมูลที่มี indicator แล้วมาคัดเลือกเป็นรายชื่อหุ้นในพอร์ตของแต่ละเดือน

import os

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (
    DEFAULT_INDICATOR_OUTPUT,
    DEFAULT_PORTFOLIO_OUTPUT,
    build_argument_parser,
    create_spark,
)


def main() -> None:
    parser = build_argument_parser("stock_select_portfolio")
    parser.add_argument("--input-path", default=str(DEFAULT_INDICATOR_OUTPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_PORTFOLIO_OUTPUT))
    parser.add_argument("--portfolio-size", type=int, default=int(os.getenv("PORTFOLIO_SIZE", "20")))
    args = parser.parse_args()

    spark = create_spark(args.app_name)
    df = spark.read.parquet(args.input_path)

    # month_end_window ใช้เก็บแค่แถวล่าสุดของแต่ละ ticker ในแต่ละเดือน
    # เพื่อให้การคัดเลือกอ้างอิง snapshot ตัวแทนของเดือนนั้นเพียงแถวเดียว
    month_end_window = Window.partitionBy("Ticker", "Year", "Month").orderBy(F.col("Date").desc())
    monthly_rank_window = Window.partitionBy("Year", "Month").orderBy(
        F.col("Volatility30").asc_nulls_last(),
        F.col("Ticker").asc(),
    )
    weight_window = Window.partitionBy("Year", "Month")

    month_end = (
        df.withColumn("month_end_rank", F.row_number().over(month_end_window))
        .filter(F.col("month_end_rank") == 1)
        .drop("month_end_rank")
    )

    # กติกาหลักของกลยุทธ์:
    # เลือกหุ้นที่แนวโน้มระยะสั้น (MA50) สูงกว่าแนวโน้มระยะยาว (MA200)
    # แล้วจัดอันดับโดยให้หุ้นที่ volatility ต่ำกว่ามาก่อน
    eligible = month_end.filter(F.col("MA50") > F.col("MA200")).filter(F.col("Volatility30").isNotNull())
    # กติกาสำรอง:
    # ถ้าไม่มีหุ้นตัวไหนผ่านเงื่อนไข trend เลย ก็ยังสร้างพอร์ตจากหุ้นที่ volatility ต่ำที่สุดแทน
    fallback = month_end.filter(F.col("Volatility30").isNotNull())

    selected_source = eligible if eligible.limit(1).count() > 0 else fallback

    selected = (
        selected_source
        .filter(F.col("Volatility30").isNotNull())
        # จัดอันดับภายในแต่ละเดือน แล้วเก็บไว้ตามจำนวนหุ้นที่ต้องการในพอร์ต
        .withColumn("selection_rank", F.row_number().over(monthly_rank_window))
        .filter(F.col("selection_rank") <= F.lit(args.portfolio_size))
        # Weight ใช้แบบ equal weight คือกระจายน้ำหนักเท่ากันทุกตัวในเดือนนั้น
        .withColumn("Constituent_Count", F.count("*").over(weight_window))
        .withColumn("Weight", F.round(F.lit(1.0) / F.col("Constituent_Count"), 6))
        .select(
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
        )
    )

    (
        selected.write.mode("overwrite")
        .partitionBy("Year", "Month")
        .parquet(args.output_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()
