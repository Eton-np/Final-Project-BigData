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
    spark_path,
)


def main() -> None:
    parser = build_argument_parser("stock_calculate_indicators")
    parser.add_argument("--input-path", default=str(DEFAULT_CLEAN_OUTPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_INDICATOR_OUTPUT))
    args = parser.parse_args()

    spark = create_spark(args.app_name)
    df = spark.read.parquet(spark_path(args.input_path))

    # rolling metrics ทุกตัวจะคำนวณแยกตาม ticker และเรียงตามเวลา
    ticker_window = Window.partitionBy("Ticker").orderBy("Date")
    cumulative_window = ticker_window.rowsBetween(Window.unboundedPreceding, 0)
    ma50_window = ticker_window.rowsBetween(-49, 0)
    ma200_window = ticker_window.rowsBetween(-199, 0)
    vol30_window = ticker_window.rowsBetween(-29, 0)
    rsi14_window = ticker_window.rowsBetween(-13, 0)
    cci14_window = ticker_window.rowsBetween(-13, 0)

    def normalized_ema(column_name: str, span: int, row_number_column: str) -> F.Column:
        alpha = 2.0 / (span + 1.0)
        beta = 1.0 - alpha
        row_num = F.col(row_number_column).cast("double")
        beta_power = F.pow(F.lit(beta), row_num)
        weighted_value = F.lit(alpha) * F.col(column_name) / beta_power
        weighted_sum = F.sum(weighted_value).over(cumulative_window)
        weighted_denominator = F.sum(F.lit(alpha) / beta_power).over(cumulative_window)
        return F.when(
            weighted_denominator.isNull() | (weighted_denominator == 0),
            F.lit(None),
        ).otherwise(weighted_sum / weighted_denominator)

    enriched = (
        # Previous_Close เป็นค่าชั่วคราวที่ใช้สำหรับคำนวณ Daily_Return
        df.withColumn("Previous_Close", F.lag("Close").over(ticker_window))
        .withColumn("Ticker_Row_Number", F.row_number().over(ticker_window))
        .withColumn(
            "Daily_Return",
            F.when(
                F.col("Previous_Close").isNull(),
                F.lit(None),
            ).otherwise((F.col("Close") - F.col("Previous_Close")) / F.col("Previous_Close")),
        )
        .withColumn(
            "Price_Change",
            F.when(F.col("Previous_Close").isNull(), F.lit(None)).otherwise(F.col("Close") - F.col("Previous_Close")),
        )
        .withColumn(
            "Gain14",
            F.when(F.col("Price_Change").isNull(), F.lit(None)).otherwise(F.greatest(F.col("Price_Change"), F.lit(0.0))),
        )
        .withColumn(
            "Loss14",
            F.when(F.col("Price_Change").isNull(), F.lit(None)).otherwise(F.abs(F.least(F.col("Price_Change"), F.lit(0.0)))),
        )
        .withColumn("MA50", F.avg("Close").over(ma50_window))
        .withColumn("MA200", F.avg("Close").over(ma200_window))
        # ส่วนเบี่ยงเบนมาตรฐาน 30 วันใช้แทนการวัด volatility แบบ rolling อย่างง่าย
        .withColumn("Volatility30", F.stddev_samp("Daily_Return").over(vol30_window))
        .withColumn("Average_Gain_14", F.avg("Gain14").over(rsi14_window))
        .withColumn("Average_Loss_14", F.avg("Loss14").over(rsi14_window))
        .withColumn(
            "RSI_14",
            F.when(F.col("Average_Loss_14").isNull(), F.lit(None))
            .when(F.col("Average_Loss_14") == 0, F.lit(100.0))
            .otherwise(
                F.lit(100.0)
                - (F.lit(100.0) / (F.lit(1.0) + (F.col("Average_Gain_14") / F.col("Average_Loss_14"))))
            ),
        )
        .withColumn("EMA_12", normalized_ema("Close", 12, "Ticker_Row_Number"))
        .withColumn("EMA_26", normalized_ema("Close", 26, "Ticker_Row_Number"))
        .withColumn("MACD_12_26_9", F.col("EMA_12") - F.col("EMA_26"))
        .withColumn("MACD_Row_Number", F.row_number().over(ticker_window))
        .withColumn("MACDs_12_26_9", normalized_ema("MACD_12_26_9", 9, "MACD_Row_Number"))
        .withColumn("MACDh_12_26_9", F.col("MACD_12_26_9") - F.col("MACDs_12_26_9"))
        .withColumn("Typical_Price", (F.col("High") + F.col("Low") + F.col("Close")) / F.lit(3.0))
        .withColumn("TP_SMA_14", F.avg("Typical_Price").over(cci14_window))
        .withColumn("TP_Deviation", F.abs(F.col("Typical_Price") - F.col("TP_SMA_14")))
        .withColumn("Mean_Deviation_14", F.avg("TP_Deviation").over(cci14_window))
        .withColumn(
            "CCI_14_0_015",
            F.when(F.col("Mean_Deviation_14").isNull() | (F.col("Mean_Deviation_14") == 0), F.lit(None))
            .otherwise((F.col("Typical_Price") - F.col("TP_SMA_14")) / (F.lit(0.015) * F.col("Mean_Deviation_14"))),
        )
        .drop(
            "Previous_Close",
            "Ticker_Row_Number",
            "Price_Change",
            "Gain14",
            "Loss14",
            "Average_Gain_14",
            "Average_Loss_14",
            "EMA_12",
            "EMA_26",
            "MACD_Row_Number",
            "Typical_Price",
            "TP_SMA_14",
            "TP_Deviation",
            "Mean_Deviation_14",
        )
        .withColumn("Year", F.year("Date"))
        .withColumn("Month", F.month("Date"))
    )

    # output ของขั้นนี้คือข้อมูลตั้งต้นสำหรับการจัดอันดับและการสร้างพอร์ต
    (
        enriched.write.mode("overwrite")
        .partitionBy("Year", "Month")
        .parquet(spark_path(args.output_path))
    )

    spark.stop()


if __name__ == "__main__":
    main()
