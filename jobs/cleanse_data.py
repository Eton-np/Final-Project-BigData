from __future__ import annotations

# ขั้นที่ 2 ของ pipeline
# job นี้ทำความสะอาดข้อมูล Parquet ดิบ โดยเติมค่าว่างเท่าที่ทำได้และลบแถวที่ใช้งานไม่ได้

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

from common import (
    DEFAULT_CLEAN_OUTPUT,
    DEFAULT_PARQUET_OUTPUT,
    build_argument_parser,
    create_spark,
    reset_output_path,
)


def main() -> None:
    parser = build_argument_parser("stock_cleanse_data")
    parser.add_argument("--input-path", default=str(DEFAULT_PARQUET_OUTPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_CLEAN_OUTPUT))
    args = parser.parse_args()

    spark = create_spark(args.app_name)
    df = spark.read.parquet(args.input_path)

    # window เหล่านี้ใช้สำหรับเติมค่าตามลำดับเวลาแยกตามแต่ละ ticker
    # เหมาะกับข้อมูลประวัติหุ้นที่บางช่วงมีค่า price ขาดหาย
    order_window = Window.partitionBy("Ticker").orderBy("Date")
    forward_fill = order_window.rowsBetween(Window.unboundedPreceding, 0)
    backward_fill = order_window.rowsBetween(0, Window.unboundedFollowing)

    numeric_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, NumericType)
        and field.name not in {"Year", "Month"}
    ]
    price_like_columns = [name for name in ["Open", "High", "Low", "Close"] if name in numeric_columns]

    # สำหรับราคา Open/High/Low/Close จะพยายามรักษาความต่อเนื่องด้วยการยืมค่าที่ไม่เป็น null ที่ใกล้ที่สุด
    for column_name in price_like_columns:
        df = df.withColumn(
            column_name,
            F.coalesce(
                F.col(column_name),
                F.last(F.col(column_name), ignorenulls=True).over(forward_fill),
                F.first(F.col(column_name), ignorenulls=True).over(backward_fill),
            ),
        )

    remaining_fill_map = {
        column_name: 0
        for column_name in numeric_columns
        if column_name not in price_like_columns
    }
    # คอลัมน์ตัวเลขที่ไม่ใช่ราคา ถ้ายังว่างอยู่จะเติมเป็น 0
    # โดยหลักจะช่วยกับคอลัมน์ลักษณะ volume หรือค่าตัวเลขประกอบอื่น ๆ
    if remaining_fill_map:
        df = df.fillna(remaining_fill_map)

    # ชั้น cleaned data จะเก็บเฉพาะแถวที่มีความหมายต่อการเทรด และลบข้อมูล ticker/date ที่ซ้ำกัน
    df = (
        df.filter(F.col("Ticker").isNotNull())
        .filter(F.col("Date").isNotNull())
        .filter(F.col("Volume") > 0)
        .dropDuplicates(["Ticker", "Date"])
        .withColumn("Year", F.year("Date"))
        .withColumn("Month", F.month("Date"))
    )

    output_path = reset_output_path(args.output_path)

    # เขียนผลลัพธ์ clean dataset ชุดใหม่ เพื่อให้ขั้นคำนวณ indicator นำไปใช้ต่อ
    (
        df.write.mode("overwrite")
        .partitionBy("Year", "Month")
        .parquet(str(output_path))
    )

    spark.stop()


if __name__ == "__main__":
    main()
