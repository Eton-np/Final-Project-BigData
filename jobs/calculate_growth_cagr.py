from __future__ import annotations

# ขั้นวิเคราะห์ถัดมาของ growth pipeline
# CAGR ใช้สรุปอัตราการเติบโตทบต้นระยะยาวระหว่างราคาเริ่มต้นกับราคาสิ้นสุด

from pyspark.sql import functions as F

from common import (
    DEFAULT_GROWTH_CAGR_OUTPUT,
    DEFAULT_GROWTH_START_END_OUTPUT,
    build_argument_parser,
    create_spark,
)


def main() -> None:
    parser = build_argument_parser("growth_calculate_cagr")
    parser.add_argument("--input-path", default=str(DEFAULT_GROWTH_START_END_OUTPUT))
    parser.add_argument("--output-path", default=str(DEFAULT_GROWTH_CAGR_OUTPUT))
    args = parser.parse_args()

    spark = create_spark(args.app_name)
    df = spark.read.parquet(args.input_path)

    # Years_Active คำนวณเป็นจำนวนปีแบบประมาณค่าเศษส่วน เพื่อให้สูตร CAGR ทำงานต่อเนื่อง
    years_active = F.datediff(F.col("End_Date"), F.col("Start_Date")) / F.lit(365.25)
    cagr = F.pow(F.col("End_Price") / F.col("Start_Price"), F.lit(1.0) / years_active) - F.lit(1.0)

    result = (
        # filter พื้นฐานเพื่อกันกรณีคณิตศาสตร์การเงินที่ไม่ถูกต้อง เช่นหารด้วยศูนย์หรือช่วงเวลาติดลบ
        df.filter(F.col("Start_Price") > 0)
        .filter(F.col("End_Price") > 0)
        .filter(F.col("End_Date") > F.col("Start_Date"))
        .withColumn("Years_Active", years_active)
        .withColumn(
            "CAGR_Percentage",
            F.when(F.col("Years_Active") > 0, cagr).otherwise(F.lit(None)),
        )
    )

    result.write.mode("overwrite").parquet(args.output_path)
    spark.stop()


if __name__ == "__main__":
    main()
