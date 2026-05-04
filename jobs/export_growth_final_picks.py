from __future__ import annotations

# ขั้น export ของ growth final picks
# ทำหน้าที่แปลงผลจัดอันดับจาก Parquet ให้เป็น CSV ไฟล์เดียว
# โดยใช้ logic สัญญาณเดียวกับหน้า /insights ก่อนเลือก top-N final picks

import csv
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.common import (
    DEFAULT_GROWTH_FINAL_PICKS_OUTPUT,
    DEFAULT_GROWTH_RANKED_OUTPUT,
    build_argument_parser,
    create_spark,
    spark_path,
)
from jobs.dashboard_datasets import enrich_growth_final_pick_rows
from jobs.dashboard_snapshot_spark import build_market_universe_rows_spark


EXPORT_FIELDS = [
    "Final_Rank",
    "Ticker",
    "Start_Date",
    "Start_Price",
    "End_Date",
    "End_Price",
    "Years_Active",
    "CAGR_Percentage",
    "Growth_Rank",
    "Decision_Score",
    "Insight_Score",
    "Insight_Label",
    "Recommendation",
    "Recommendation_Key",
    "Signal",
    "RSI_14",
    "MACD_12_26_9",
    "Range_Pct",
    "Reasons",
    "Warnings",
]


def _timestamp_to_text(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _load_growth_ranked_rows(input_path: str) -> list[dict]:
    spark = create_spark("growth_final_picks_export")
    try:
        rows = (
            spark.read.parquet(spark_path(input_path))
            .orderBy("Growth_Rank")
            .select(
                "Ticker",
                "Start_Date",
                "Start_Price",
                "End_Date",
                "End_Price",
                "Years_Active",
                "CAGR_Percentage",
                "Growth_Rank",
            )
            .collect()
        )
    finally:
        spark.stop()

    return [
        {
            "ticker": row["Ticker"],
            "start_date": _timestamp_to_text(row["Start_Date"]),
            "start_price": row["Start_Price"],
            "end_date": _timestamp_to_text(row["End_Date"]),
            "end_price": row["End_Price"],
            "years_active": row["Years_Active"],
            "cagr_percentage": row["CAGR_Percentage"],
            "growth_rank": row["Growth_Rank"],
            "return_multiple": (row["End_Price"] / row["Start_Price"])
            if row["Start_Price"] and row["Start_Price"] > 0 and row["End_Price"]
            else None,
        }
        for row in rows
        if row["Ticker"]
    ]


def _pick_export_rows(enriched_rows: list[dict], top_n: int) -> list[dict]:
    # CSV final picks ควรเป็นหุ้นที่ผ่าน logic เดียวกับหน้า insights ก่อน
    # ถ้ารอบใดมีตัวผ่านไม่ถึง top_n จึงค่อยเติมตัวรองลงมาด้วยคะแนนรวมสูงสุด
    ready_rows = [row for row in enriched_rows if row.get("recommendation_key") == "consider_first"]
    fallback_rows = [row for row in enriched_rows if row.get("recommendation_key") != "consider_first"]
    return (ready_rows + fallback_rows)[:top_n]


def _to_export_row(row: dict, final_rank: int) -> dict:
    return {
        "Final_Rank": final_rank,
        "Ticker": row.get("ticker"),
        "Start_Date": row.get("start_date"),
        "Start_Price": row.get("start_price"),
        "End_Date": row.get("end_date"),
        "End_Price": row.get("end_price"),
        "Years_Active": row.get("years_active"),
        "CAGR_Percentage": row.get("cagr_percentage"),
        "Growth_Rank": row.get("growth_rank"),
        "Decision_Score": row.get("decision_score"),
        "Insight_Score": row.get("insight_score"),
        "Insight_Label": row.get("insight_label"),
        "Recommendation": row.get("recommendation"),
        "Recommendation_Key": row.get("recommendation_key"),
        "Signal": row.get("signal"),
        "RSI_14": row.get("rsi_14"),
        "MACD_12_26_9": row.get("macd_12_26_9"),
        "Range_Pct": row.get("range_pct"),
        "Reasons": json.dumps(row.get("reasons") or [], ensure_ascii=False),
        "Warnings": json.dumps(row.get("warnings") or [], ensure_ascii=False),
    }


def main() -> None:
    parser = build_argument_parser("growth_final_picks_export")
    parser.add_argument("--input-path", default=str(DEFAULT_GROWTH_RANKED_OUTPUT))
    parser.add_argument("--output-file", default=str(DEFAULT_GROWTH_FINAL_PICKS_OUTPUT))
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument(
        "--candidate-pool-size",
        type=int,
        default=140,
        help="Limit growth-ranked rows before signal scoring. 0 means score the full growth universe.",
    )
    args = parser.parse_args()

    growth_rows = _load_growth_ranked_rows(args.input_path)
    if args.candidate_pool_size > 0:
        growth_rows = growth_rows[: args.candidate_pool_size]

    market_rows = build_market_universe_rows_spark()
    enriched_rows = enrich_growth_final_pick_rows(growth_rows, market_rows)
    export_rows = _pick_export_rows(enriched_rows, args.top_n)

    output_file = Path(args.output_file).resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=EXPORT_FIELDS)
        writer.writeheader()
        for index, row in enumerate(export_rows, start=1):
            writer.writerow(_to_export_row(row, index))


if __name__ == "__main__":
    main()
