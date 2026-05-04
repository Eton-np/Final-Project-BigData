from __future__ import annotations

import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

try:
    from pyspark.sql import Window
    from pyspark.sql import functions as F
except ModuleNotFoundError:
    Window = None
    F = None

from jobs.common import (
    DEFAULT_CLEAN_OUTPUT,
    DEFAULT_GROWTH_RANKED_OUTPUT,
    DEFAULT_INDICATOR_OUTPUT,
    DEFAULT_PARQUET_OUTPUT,
    DEFAULT_PORTFOLIO_OUTPUT,
    create_spark,
    spark_path,
)
from jobs.dashboard_datasets import (
    build_growth_final_picks_summary,
    enrich_growth_final_pick_rows,
    load_growth_final_pick_rows,
)


def _safe_float(value: str | None) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _safe_int(value: str | None) -> int | None:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _timestamp_to_text(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _rsi_bucket(rsi: float | None) -> str:
    if rsi is None:
        return "Unknown"
    if rsi >= 70:
        return "Overbought"
    if rsi >= 55:
        return "Strength"
    if rsi >= 45:
        return "Balanced"
    if rsi >= 30:
        return "Weakening"
    return "Oversold"


def _signal_label(row: dict[str, Any]) -> str:
    return_pct = row["return_pct"] or 0.0
    rsi = row["rsi_14"]
    macd = row["macd_12_26_9"]
    if return_pct >= 0.02 and (rsi is None or rsi >= 55) and (macd is None or macd >= 0):
        return "Momentum"
    if return_pct <= -0.02 or (rsi is not None and rsi < 40):
        return "Pressure"
    if rsi is not None and 45 <= rsi <= 55:
        return "Balanced"
    return "Watch"


def _scale(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _insight_score(row: dict[str, Any]) -> tuple[float, list[str], list[str]]:
    score = 50.0
    reasons: list[str] = []
    warnings: list[str] = []

    if row["in_portfolio"]:
        score += 10
        reasons.append("Already selected in the core portfolio")
    if row["in_growth_watchlist"]:
        score += 12
        reasons.append("Also appears in the tracked growth universe")

    return_pct = row["return_pct"] or 0.0
    range_pct = row["range_pct"] or 0.0
    rsi = row["rsi_14"]
    macd = row["macd_12_26_9"]
    price = row["close"] or 0.0
    cci = row["cci_14_0_015"]

    score += _scale(return_pct * 250, -18, 18)
    if return_pct >= 0.02:
        reasons.append("Strong historical return profile")
    elif return_pct <= -0.02:
        warnings.append("Historical return profile is weak")

    score -= _scale(range_pct * 120, 0, 14)
    if range_pct >= 0.08:
        warnings.append("Wide trading range signals higher risk")

    if rsi is not None:
        if 52 <= rsi <= 68:
            score += 8
            reasons.append("RSI sits in a healthy momentum zone")
        elif rsi > 70:
            score -= 6
            warnings.append("RSI is overbought")
        elif rsi < 35:
            score -= 4
            warnings.append("RSI remains weak")

    if macd is not None:
        if macd > 0:
            score += 6
            reasons.append("MACD remains above zero")
        else:
            score -= 5
            warnings.append("MACD is still negative")

    if cci is not None and cci > 100:
        score += 4
        reasons.append("CCI confirms upside acceleration")
    elif cci is not None and cci < -100:
        score -= 4
        warnings.append("CCI shows downside stress")

    if price < 5:
        score -= 8
        warnings.append("Low price profile increases speculative risk")

    return _scale(score, 0, 100), reasons[:3], warnings[:3]


def _insight_label(score: float) -> str:
    if score >= 68:
        return "Worth Watching"
    if score >= 48:
        return "Stable"
    return "Caution"


def _without_history_metrics(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key not in {"returns_by_year", "returns_by_years"}}


def _load_history_frame(spark, years_back: int):
    preferred_path = None
    for candidate_path in (DEFAULT_INDICATOR_OUTPUT, DEFAULT_CLEAN_OUTPUT, DEFAULT_PARQUET_OUTPUT):
        parquet_files = list(candidate_path.rglob("*.parquet")) if candidate_path.exists() else []
        if parquet_files:
            preferred_path = candidate_path
            break
    if preferred_path is None:
        return None, None

    df = spark.read.parquet(spark_path(preferred_path))
    latest_date = df.select(F.max("Date").alias("latest_date")).first()["latest_date"]
    if latest_date is None:
        return None, None
    if years_back > 0:
        cutoff_date = spark.range(1).select(F.add_months(F.lit(latest_date), -(years_back * 12)).alias("cutoff")).first()[
            "cutoff"
        ]
        df = df.filter(F.col("Date") >= F.lit(cutoff_date))
        latest_date = df.select(F.max("Date").alias("latest_date")).first()["latest_date"]
    if "Year" not in df.columns:
        df = df.withColumn("Year", F.year("Date"))
    return df.cache(), latest_date


def _read_portfolio_lookup(spark, path: Path = DEFAULT_PORTFOLIO_OUTPUT) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    parquet_files = list(path.rglob("*.parquet"))
    if not parquet_files:
        return {}
    df = (
        spark.read.parquet(spark_path(path))
        .withColumn("year", F.col("Year").cast("int"))
        .withColumn("month", F.col("Month").cast("int"))
        .withColumn("ticker", F.upper(F.trim(F.col("Ticker"))))
        .withColumn("weight", F.col("Weight").cast("double"))
        .withColumn("selection_rank", F.col("selection_rank").cast("int"))
    )
    rows = df.collect()
    if not rows:
        return {}
    latest_key = max((row["year"] or 0, row["month"] or 0, row["Date"] or "") for row in rows)
    return {
        row["ticker"]: {
            "date": row["Date"],
            "weight": row["weight"],
            "selection_rank": row["selection_rank"],
        }
        for row in rows
        if (row["year"] or 0, row["month"] or 0, row["Date"] or "") == latest_key and row["ticker"]
    }


def _read_growth_lookup(spark, path: Path = DEFAULT_GROWTH_RANKED_OUTPUT) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    parquet_files = list(path.rglob("*.parquet"))
    if not parquet_files:
        return {}
    rows = (
        spark.read.parquet(spark_path(path))
        .withColumn("ticker", F.upper(F.trim(F.col("Ticker"))))
        .withColumn("growth_rank", F.col("Growth_Rank").cast("int"))
        .withColumn("cagr_percentage", F.col("CAGR_Percentage").cast("double"))
        .collect()
    )
    return {
        row["ticker"]: {"growth_rank": row["growth_rank"], "cagr_percentage": row["cagr_percentage"]}
        for row in rows
        if row["ticker"]
    }


def _column_or_null(df, column_name: str):
    if column_name in df.columns:
        return F.col(column_name)
    return F.lit(None).cast("double")


def _pyspark_is_available() -> bool:
    return Window is not None and F is not None


def _csv_fallback_enabled() -> bool:
    return os.getenv("DASHBOARD_ALLOW_CSV_FALLBACK", "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_market_universe_rows_without_spark() -> list[dict[str, Any]]:
    from jobs.dashboard_datasets import load_market_universe

    return load_market_universe()


def build_market_universe_rows_spark(
    input_path: str | None = None,
    max_files: int | None = None,
    years_back: int | None = None,
) -> list[dict[str, Any]]:
    if not _pyspark_is_available():
        if _csv_fallback_enabled():
            return _load_market_universe_rows_without_spark()
        raise RuntimeError("PySpark is required to build dashboard snapshots from Parquet")

    try:
        spark = create_spark("dashboard_snapshot")
    except ModuleNotFoundError:
        if _csv_fallback_enabled():
            return _load_market_universe_rows_without_spark()
        raise

    try:
        years_back_value = years_back if years_back is not None else int(os.getenv("RAW_INPUT_YEARS_BACK", "10"))
        history_df, latest_date = _load_history_frame(spark, years_back_value)
        if history_df is None or latest_date is None:
            return []

        latest_window = Window.partitionBy("Ticker").orderBy(F.col("Date").desc())
        latest_selects = [
            F.col("Ticker"),
            F.col("Date"),
            F.col("Open"),
            F.col("High"),
            F.col("Low"),
            F.col("Close"),
            F.col("Volume"),
            _column_or_null(history_df, "RSI_14").alias("RSI_14"),
            _column_or_null(history_df, "MACD_12_26_9").alias("MACD_12_26_9"),
            _column_or_null(history_df, "CCI_14_0_015").alias("CCI_14_0_015"),
        ]
        latest_rows = (
            history_df.filter(F.col("Date") == F.lit(latest_date))
            .withColumn("latest_rank", F.row_number().over(latest_window))
            .filter(F.col("latest_rank") == 1)
            .drop("latest_rank")
            .select(*latest_selects)
            .collect()
        )

        year_start = max(latest_date.year - max(years_back_value, 1) + 1, 1)
        yearly_df = history_df.filter((F.col("Year") >= F.lit(year_start)) & (F.col("Year") <= F.lit(latest_date.year)))
        first_window = Window.partitionBy("Ticker", "Year").orderBy(F.col("Date").asc(), F.col("Close").asc_nulls_last())
        last_window = Window.partitionBy("Ticker", "Year").orderBy(F.col("Date").desc(), F.col("Close").desc_nulls_last())
        first_rows = (
            yearly_df.withColumn("first_rank", F.row_number().over(first_window))
            .filter(F.col("first_rank") == 1)
            .select(
                "Ticker",
                "Year",
                F.col("Date").alias("start_date"),
                F.col("Close").alias("start_close"),
            )
        )
        last_rows = (
            yearly_df.withColumn("last_rank", F.row_number().over(last_window))
            .filter(F.col("last_rank") == 1)
            .select(
                "Ticker",
                "Year",
                F.col("Date").alias("end_date"),
                F.col("Close").alias("close"),
                F.col("Volume").alias("volume"),
                _column_or_null(yearly_df, "RSI_14").alias("rsi_14"),
                _column_or_null(yearly_df, "MACD_12_26_9").alias("macd_12_26_9"),
                _column_or_null(yearly_df, "CCI_14_0_015").alias("cci_14_0_015"),
                F.col("Open").alias("open_price"),
                F.col("High").alias("high_price"),
                F.col("Low").alias("low_price"),
            )
        )
        yearly_metrics_rows = (
            first_rows.join(last_rows, ["Ticker", "Year"], "inner")
            .filter(F.col("start_close").isNotNull() & F.col("close").isNotNull() & (F.col("start_close") > 0))
            .collect()
        )

        returns_by_year_lookup: dict[str, dict[str, Any]] = {}
        for row in yearly_metrics_rows:
            range_pct = None
            if row["open_price"] not in {None, 0} and row["high_price"] is not None and row["low_price"] is not None:
                range_pct = (row["high_price"] - row["low_price"]) / row["open_price"]
            metric = {
                "return_pct": (row["close"] - row["start_close"]) / row["start_close"],
                "start_date": _timestamp_to_text(row["start_date"]),
                "end_date": _timestamp_to_text(row["end_date"]),
                "start_close": row["start_close"],
                "close": row["close"],
                "volume": row["volume"] or 0.0,
                "rsi_14": row["rsi_14"],
                "macd_12_26_9": row["macd_12_26_9"],
                "cci_14_0_015": row["cci_14_0_015"],
                "range_pct": range_pct,
                "rsi_bucket": _rsi_bucket(row["rsi_14"]),
            }
            returns_by_year_lookup.setdefault(row["Ticker"], {})[str(row["Year"])] = metric

        portfolio_lookup = _read_portfolio_lookup(spark)
        growth_lookup = _read_growth_lookup(spark)

        selected_year = latest_date.year
        rows: list[dict[str, Any]] = []
        for raw in latest_rows:
            ticker = raw["Ticker"]
            annual_metrics = returns_by_year_lookup.get(ticker, {})
            selected_metric = annual_metrics.get(str(selected_year))
            open_price = raw["Open"]
            close_price = raw["Close"]
            high_price = raw["High"]
            low_price = raw["Low"]
            range_pct = None
            if open_price not in {None, 0} and high_price is not None and low_price is not None:
                range_pct = (high_price - low_price) / open_price
            portfolio_row = portfolio_lookup.get(ticker)
            growth_row = growth_lookup.get(ticker)
            row = {
                "ticker": ticker,
                "date": _timestamp_to_text(raw["Date"]),
                "history_start_date": selected_metric.get("start_date") if selected_metric else None,
                "history_start_close": selected_metric.get("start_close") if selected_metric else None,
                "history_lookback_years": years_back_value,
                "selected_year": selected_year,
                "history_years_active": 1.0 if selected_metric else None,
                "total_return_pct": selected_metric.get("return_pct") if selected_metric else None,
                "returns_by_year": annual_metrics,
                "open": open_price,
                "close": close_price,
                "high": high_price,
                "low": low_price,
                "volume": raw["Volume"] or 0.0,
                "rsi_14": raw["RSI_14"],
                "macd_12_26_9": raw["MACD_12_26_9"],
                "cci_14_0_015": raw["CCI_14_0_015"],
                "return_pct": selected_metric.get("return_pct") if selected_metric else None,
                "range_pct": selected_metric.get("range_pct") if selected_metric else range_pct,
                "price_band": (
                    "Large Cap Proxy"
                    if (close_price or 0) >= 100
                    else "Mid Price"
                    if (close_price or 0) >= 20
                    else "Active Small Cap"
                    if (close_price or 0) >= 5
                    else "Speculative"
                ),
                "rsi_bucket": _rsi_bucket(raw["RSI_14"]),
                "signal": "Unknown",
                "in_portfolio": portfolio_row is not None,
                "in_growth_watchlist": growth_row is not None,
                "portfolio_weight": portfolio_row["weight"] if portfolio_row else None,
                "selection_rank": portfolio_row["selection_rank"] if portfolio_row else None,
                "growth_rank": growth_row["growth_rank"] if growth_row else None,
                "growth_cagr": growth_row["cagr_percentage"] if growth_row else None,
            }
            row["signal"] = _signal_label(row)
            rows.append(row)

        return rows
    finally:
        spark.stop()


def build_market_dashboard_dataset_spark() -> dict[str, Any]:
    rows = build_market_universe_rows_spark()
    returns = [row["return_pct"] for row in rows if row["return_pct"] is not None]
    rsis = [row["rsi_14"] for row in rows if row["rsi_14"] is not None]
    ranges = [row["range_pct"] for row in rows if row["range_pct"] is not None]
    volumes = [row["volume"] for row in rows]

    advancers = sum(1 for value in returns if value > 0)
    decliners = sum(1 for value in returns if value < 0)
    unchanged = max(len(rows) - advancers - decliners, 0)
    breadth_ratio = (advancers / decliners) if decliners else float(advancers or 0)

    top_gainers = sorted(
        [row for row in rows if row["return_pct"] is not None],
        key=lambda row: (row["return_pct"], row["volume"]),
        reverse=True,
    )[:8]
    top_losers = sorted(
        [row for row in rows if row["return_pct"] is not None],
        key=lambda row: (row["return_pct"], -row["volume"]),
    )[:8]
    most_active = sorted(rows, key=lambda row: (row["volume"], abs(row["return_pct"] or 0.0)), reverse=True)[:8]
    heatmap_rows = sorted(
        [row for row in rows if row["return_pct"] is not None],
        key=lambda row: (abs(row["return_pct"]), row["volume"]),
        reverse=True,
    )[:36]
    scatter_rows = sorted(rows, key=lambda row: (row["volume"], abs(row["return_pct"] or 0.0)), reverse=True)[:120]

    signal_counts = Counter(row["signal"] for row in rows)
    price_band_counts = Counter(row["price_band"] for row in rows)
    rsi_bucket_counts = Counter(row["rsi_bucket"] for row in rows)
    available_years = sorted(
        {
            int(year)
            for row in rows
            for year in (row.get("returns_by_year") or {}).keys()
            if str(year).isdigit()
        },
        reverse=True,
    )
    selected_year = max(available_years) if available_years else 0

    summary = {
        "tracked_count": len(rows),
        "advancers": advancers,
        "decliners": decliners,
        "unchanged": unchanged,
        "average_return": _round(sum(returns) / len(returns) if returns else 0.0),
        "median_rsi": _round(median(rsis) if rsis else 0.0, 2),
        "average_range_pct": _round(sum(ranges) / len(ranges) if ranges else 0.0),
        "total_volume": int(sum(volumes)),
        "breadth_ratio": _round(breadth_ratio, 2),
        "portfolio_overlap": sum(1 for row in rows if row["in_portfolio"]),
        "growth_overlap": sum(1 for row in rows if row["in_growth_watchlist"]),
        "pipeline_lookback_years": _safe_int(os.getenv("RAW_INPUT_YEARS_BACK")) or 10,
        "selected_year": selected_year,
    }

    highlights = {
        "strongest_momentum": top_gainers[0]["ticker"] if top_gainers else None,
        "highest_turnover": most_active[0]["ticker"] if most_active else None,
        "deepest_pullback": top_losers[0]["ticker"] if top_losers else None,
        "broadest_theme": signal_counts.most_common(1)[0][0] if signal_counts else None,
    }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_as_of": max((row["date"] for row in rows if row.get("date")), default=None),
        "summary": summary,
        "highlights": highlights,
        "signals": [{"label": key, "count": value} for key, value in signal_counts.items()],
        "price_bands": [{"label": key, "count": value} for key, value in price_band_counts.items()],
        "rsi_buckets": [{"label": key, "count": value} for key, value in rsi_bucket_counts.items()],
        "top_gainers": [_without_history_metrics(row) for row in top_gainers],
        "top_losers": [_without_history_metrics(row) for row in top_losers],
        "most_active": [_without_history_metrics(row) for row in most_active],
        "heatmap": [_without_history_metrics(row) for row in heatmap_rows],
        "scatter": [_without_history_metrics(row) for row in scatter_rows],
        "securities": rows,
        "filters": {
            "signals": sorted(signal_counts.keys()),
            "price_bands": sorted(price_band_counts.keys()),
            "rsi_buckets": sorted(rsi_bucket_counts.keys()),
            "lookback_years": [1, 3, 5, 10],
            "years": available_years,
        },
        "empty_state": len(rows) == 0,
    }


def build_investment_insights_dataset_spark() -> dict[str, Any]:
    rows = build_market_universe_rows_spark()
    enriched: list[dict[str, Any]] = []
    label_counts: Counter[str] = Counter()
    for row in rows:
        score, reasons, warnings = _insight_score(row)
        label = _insight_label(score)
        label_counts[label] += 1
        enriched.append(
            {
                **row,
                "insight_score": round(score, 2),
                "insight_label": label,
                "reasons": reasons,
                "warnings": warnings,
            }
        )

    ranked = sorted(
        enriched,
        key=lambda row: (
            row["insight_score"],
            row["in_growth_watchlist"],
            row["in_portfolio"],
            row["volume"],
        ),
        reverse=True,
    )
    worth_watching = [row for row in ranked if row["insight_label"] == "Worth Watching"]
    caution_ranked = [row for row in ranked if row["insight_label"] == "Caution"]
    stable_ranked = [row for row in ranked if row["insight_label"] == "Stable"]
    spotlight = worth_watching[:30]
    candidates = worth_watching[:60] + stable_ranked[:40] + caution_ranked[-40:]
    caution_list = caution_ranked[-8:]
    stable_names = stable_ranked[:8]
    growth_final_picks = enrich_growth_final_pick_rows(load_growth_final_pick_rows(), enriched)

    summary = {
        "tracked_count": len(enriched),
        "worth_watching": label_counts["Worth Watching"],
        "stable": label_counts["Stable"],
        "caution": label_counts["Caution"],
        "average_score": _round(sum(row["insight_score"] for row in enriched) / len(enriched) if enriched else 0.0, 2),
        "top_score": ranked[0]["insight_score"] if ranked else None,
        "portfolio_overlap": sum(1 for row in enriched if row["in_portfolio"]),
        "growth_overlap": sum(1 for row in enriched if row["in_growth_watchlist"]),
        "pipeline_lookback_years": _safe_int(os.getenv("RAW_INPUT_YEARS_BACK")) or 10,
    }
    model_notes = [
        {
            "title": "Signal Model",
            "body": "The insights score blends historical annualized return, latest range, RSI, MACD, CCI, and membership in the existing portfolio watchlists.",
        },
        {
            "title": "Interpretation",
            "body": "Worth Watching names combine better momentum with cleaner risk signals, Stable names look mixed, and Caution names show heavier downside pressure or speculative risk.",
        },
        {
            "title": "Workflow",
            "body": "Airflow refreshes these JSON outputs so the dashboard reads the same prepared snapshot every time the pipeline completes.",
        },
    ]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_as_of": max((row["date"] for row in enriched if row.get("date")), default=None),
        "summary": summary,
        "score_distribution": [
            {"label": key, "count": value}
            for key, value in (
                ("Worth Watching", label_counts["Worth Watching"]),
                ("Stable", label_counts["Stable"]),
                ("Caution", label_counts["Caution"]),
            )
        ],
        "candidates": [_without_history_metrics(row) for row in candidates],
        "spotlight": [_without_history_metrics(row) for row in spotlight],
        "growth_final_picks": growth_final_picks,
        "growth_final_picks_summary": build_growth_final_picks_summary(growth_final_picks),
        "worth_watching": [_without_history_metrics(row) for row in worth_watching[:8]],
        "stable_watch": [_without_history_metrics(row) for row in stable_names],
        "caution_list": [_without_history_metrics(row) for row in caution_list],
        "model_notes": model_notes,
        "filters": {
            "labels": ["Worth Watching", "Stable", "Caution"],
            "membership": ["Portfolio", "Growth", "Unassigned"],
        },
        "empty_state": len(enriched) == 0,
    }
