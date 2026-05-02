from __future__ import annotations

# ขั้นจัดอันดับของกลยุทธ์ growth
# job นี้กรองหุ้นที่มีประวัติสั้นเกินไปหรือราคาต่ำเกินไปออก ก่อนจัดอันดับด้วย CAGR

import os

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (
    DEFAULT_GROWTH_CAGR_OUTPUT,
    DEFAULT_GROWTH_RANKED_OUTPUT,
    build_argument_parser,
    create_spark,
    spark_path,
)


def main() -> None:
    parser = build_argument_parser("growth_filter_rank")
    parser.add_argument("--input-path", default=str(DEFAULT_GROWTH_CAGR_OUTPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_GROWTH_RANKED_OUTPUT))
    parser.add_argument("--minimum-years", type=float, default=float(os.getenv("GROWTH_MINIMUM_YEARS", "1")))
    parser.add_argument("--minimum-price", type=float, default=float(os.getenv("GROWTH_MINIMUM_PRICE", "5")))
    args = parser.parse_args()

    spark = create_spark(args.app_name)
    df = spark.read.parquet(spark_path(args.input_path))

    result = (
        # minimum years และ minimum price ทำหน้าที่เป็น quality filter ก่อนเริ่มจัดอันดับ
        df.filter(F.col("Years_Active") >= F.lit(args.minimum_years))
        .filter(F.col("End_Price") >= F.lit(args.minimum_price))
        .filter(F.col("CAGR_Percentage").isNotNull())
        .orderBy(F.col("CAGR_Percentage").desc(), F.col("Ticker").asc())
        # Growth_Rank คืออันดับสุดท้ายที่ export layer และ dashboard จะนำไปใช้ต่อ
        .withColumn("Growth_Rank", F.row_number().over(Window.orderBy(F.col("CAGR_Percentage").desc(), F.col("Ticker").asc())))
    )

    result.write.mode("overwrite").parquet(spark_path(args.output_path))
    spark.stop()


if __name__ == "__main__":
    main()
