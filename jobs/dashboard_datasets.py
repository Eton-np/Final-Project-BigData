from __future__ import annotations

# โมดูลนี้เป็นตัวเชื่อมระหว่างผลลัพธ์จาก data engineering กับหน้า dashboard
# ทำหน้าที่โหลด CSV ที่เตรียมไว้, คำนวณ metric ที่ใช้แสดงผล และสร้าง JSON payload ให้ web layer

import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from jobs.common import (
    DEFAULT_EXPORT_OUTPUT,
    DEFAULT_GROWTH_EXPORT_OUTPUT,
    DEFAULT_INVESTMENT_INSIGHTS_OUTPUT,
    DEFAULT_MARKET_DASHBOARD_OUTPUT,
    PROJECT_ROOT,
)


TODAYS_STOCKS_PATH = PROJECT_ROOT / "Data" / "Todays_stocks.csv"


def _safe_float(value: str | None) -> float | None:
    # parse แบบระวัง error เพื่อไม่ให้ค่าที่ผิดรูปเพียงช่องเดียวทำให้การสร้าง dashboard ล้มทั้งชุด
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


def _scale(value: float, lower: float, upper: float) -> float:
    # helper สำหรับบีบค่า score ให้อยู่ในช่วงที่กำหนด
    return max(lower, min(upper, value))


def _signal_label(row: dict[str, Any]) -> str:
    # label แบบ heuristic อย่างง่ายสำหรับสรุปสภาพระยะสั้นของหุ้นในหน้า Market Dashboard
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


def _rsi_bucket(rsi: float | None) -> str:
    # แปลงค่า RSI ดิบให้เป็นหมวดที่อ่านง่ายและเหมาะกับ filter/summary บนหน้าเว็บ
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


def _load_latest_portfolio_rows(portfolio_csv: Path = DEFAULT_EXPORT_OUTPUT) -> list[dict[str, Any]]:
    # dashboard สนใจเฉพาะ snapshot พอร์ตล่าสุด จึงโหลดเฉพาะข้อมูลเดือนล่าสุดมาใช้
    if not portfolio_csv.exists():
        return []

    rows: list[dict[str, Any]] = []
    with portfolio_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            rows.append(
                {
                    "date": raw.get("Date"),
                    "year": _safe_int(raw.get("Year")) or 0,
                    "month": _safe_int(raw.get("Month")) or 0,
                    "ticker": (raw.get("Ticker") or "").strip(),
                    "close": _safe_float(raw.get("Close")) or 0.0,
                    "weight": _safe_float(raw.get("Weight")) or 0.0,
                    "selection_rank": _safe_int(raw.get("selection_rank")) or 0,
                }
            )

    if not rows:
        return []

    latest_key = max((row["year"], row["month"], row["date"]) for row in rows)
    return sorted(
        [row for row in rows if (row["year"], row["month"], row["date"]) == latest_key],
        key=lambda row: (row["selection_rank"], row["ticker"]),
    )


def _load_growth_rows(growth_csv: Path = DEFAULT_GROWTH_EXPORT_OUTPUT) -> list[dict[str, Any]]:
    # โหลด growth watchlist ที่ export ไว้ เพื่อใช้เช็กว่าซ้ำกับหุ้นในตลาดชุดปัจจุบันหรือไม่
    if not growth_csv.exists():
        return []

    rows: list[dict[str, Any]] = []
    with growth_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            ticker = (raw.get("Ticker") or "").strip()
            if not ticker:
                continue
            rows.append(
                {
                    "ticker": ticker,
                    "growth_rank": _safe_int(raw.get("Growth_Rank")) or 0,
                    "cagr_percentage": _safe_float(raw.get("CAGR_Percentage")) or 0.0,
                    "years_active": _safe_float(raw.get("Years_Active")) or 0.0,
                    "end_price": _safe_float(raw.get("End_Price")) or 0.0,
                }
            )
    return sorted(rows, key=lambda row: (row["growth_rank"], row["ticker"]))


def load_market_universe(todays_csv: Path = TODAYS_STOCKS_PATH) -> list[dict[str, Any]]:
    # ฟังก์ชันนี้ประกอบ market universe พื้นฐานของ dashboard จาก 3 แหล่ง
    # 1) snapshot ตลาดวันนี้
    # 2) portfolio export ล่าสุด
    # 3) growth export ล่าสุด
    portfolio_rows = _load_latest_portfolio_rows()
    growth_rows = _load_growth_rows()
    portfolio_lookup = {row["ticker"]: row for row in portfolio_rows}
    growth_lookup = {row["ticker"]: row for row in growth_rows}

    if not todays_csv.exists():
        return []

    rows: list[dict[str, Any]] = []
    with todays_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            ticker = (raw.get("Stock") or "").strip()
            if not ticker:
                continue

            open_price = _safe_float(raw.get("Open"))
            close_price = _safe_float(raw.get("Close"))
            high_price = _safe_float(raw.get("High"))
            low_price = _safe_float(raw.get("Low"))
            volume = _safe_float(raw.get("Volume")) or 0.0
            rsi_14 = _safe_float(raw.get("RSI_14"))
            macd = _safe_float(raw.get("MACD_12_26_9"))
            cci = _safe_float(raw.get("CCI_14_0.015"))

            return_pct = None
            if open_price not in {None, 0} and close_price is not None:
                return_pct = (close_price - open_price) / open_price

            range_pct = None
            if open_price not in {None, 0} and high_price is not None and low_price is not None:
                range_pct = (high_price - low_price) / open_price

            portfolio_row = portfolio_lookup.get(ticker)
            growth_row = growth_lookup.get(ticker)

            # แต่ละแถวจะถูกแปลงให้เป็น object ที่มีข้อมูลพร้อมใช้บน dashboard
            # รวมถึง label ที่คำนวณเพิ่ม และ flag สำหรับบอกว่าอยู่ใน portfolio/growth หรือไม่
            row = {
                "ticker": ticker,
                "date": raw.get("Date"),
                "open": open_price,
                "close": close_price,
                "high": high_price,
                "low": low_price,
                "volume": volume,
                "rsi_14": rsi_14,
                "macd_12_26_9": macd,
                "cci_14_0_015": cci,
                "return_pct": return_pct,
                "range_pct": range_pct,
                "price_band": (
                    "Large Cap Proxy"
                    if (close_price or 0) >= 100
                    else "Mid Price"
                    if (close_price or 0) >= 20
                    else "Active Small Cap"
                    if (close_price or 0) >= 5
                    else "Speculative"
                ),
                "rsi_bucket": _rsi_bucket(rsi_14),
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


def build_market_dashboard_dataset() -> dict[str, Any]:
    # สร้าง payload สำหรับหน้า Market Dashboard หลัก
    # แนวคิดคือคำนวณสรุปที่หน้า UI ต้องใช้ไว้ล่วงหน้า เพื่อให้ฝั่งเว็บทำงานง่ายและตอบสนองเร็ว
    rows = load_market_universe()
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
        "top_gainers": top_gainers,
        "top_losers": top_losers,
        "most_active": most_active,
        "heatmap": heatmap_rows,
        "scatter": scatter_rows,
        "securities": rows,
        "filters": {
            "signals": sorted(signal_counts.keys()),
            "price_bands": sorted(price_band_counts.keys()),
            "rsi_buckets": sorted(rsi_bucket_counts.keys()),
        },
        "empty_state": len(rows) == 0,
    }


def _insight_score(row: dict[str, Any]) -> tuple[float, list[str], list[str]]:
    # โมเดลให้คะแนนสำหรับหน้า Investment Insights
    # ผสมทั้ง momentum, risk, technical indicators และการอยู่ใน watchlist เข้าด้วยกัน
    score = 50.0
    reasons: list[str] = []
    warnings: list[str] = []

    if row["in_portfolio"]:
        score += 10
        reasons.append("Already selected in the core portfolio")
    if row["in_growth_watchlist"]:
        score += 12
        reasons.append("Also appears in the growth watchlist")

    return_pct = row["return_pct"] or 0.0
    range_pct = row["range_pct"] or 0.0
    rsi = row["rsi_14"]
    macd = row["macd_12_26_9"]
    price = row["close"] or 0.0
    cci = row["cci_14_0_015"]

    score += _scale(return_pct * 250, -18, 18)
    if return_pct >= 0.02:
        reasons.append("Strong intraday move")
    elif return_pct <= -0.02:
        warnings.append("Selling pressure is elevated")

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

    # จำกัดช่วง score และจำนวนข้อความอธิบาย เพื่อให้ผลลัพธ์อ่านง่ายบนหน้า UI
    score = _scale(score, 0, 100)
    return score, reasons[:3], warnings[:3]


def _insight_label(score: float) -> str:
    # label สุดท้ายที่เป็นภาษาคนอ่าน ใช้ทั้งในการ์ด, filter และตาราง
    if score >= 68:
        return "Worth Watching"
    if score >= 48:
        return "Stable"
    return "Caution"


def build_investment_insights_dataset() -> dict[str, Any]:
    # สร้าง payload สำหรับหน้า dashboard ที่สอง ซึ่งเน้นการจัดอันดับไอเดียลงทุน
    rows = load_market_universe()
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

    spotlight = ranked[:30]
    candidates = ranked[:120]
    # ตัดชุดข้อมูลย่อยไว้ล่วงหน้า เพื่อให้แต่ละ section ของ UI ใช้งานได้ทันทีโดยไม่ต้องจัดอันดับใหม่ใน browser
    worth_watching = [row for row in ranked if row["insight_label"] == "Worth Watching"][:8]
    caution_list = [row for row in ranked if row["insight_label"] == "Caution"][-8:]
    stable_names = [row for row in ranked if row["insight_label"] == "Stable"][:8]

    summary = {
        "tracked_count": len(enriched),
        "worth_watching": label_counts["Worth Watching"],
        "stable": label_counts["Stable"],
        "caution": label_counts["Caution"],
        "average_score": _round(sum(row["insight_score"] for row in enriched) / len(enriched) if enriched else 0.0, 2),
        "top_score": ranked[0]["insight_score"] if ranked else None,
        "portfolio_overlap": sum(1 for row in enriched if row["in_portfolio"]),
        "growth_overlap": sum(1 for row in enriched if row["in_growth_watchlist"]),
    }

    model_notes = [
        {
            "title": "Signal Model",
            "body": "The insights score blends intraday return, range, RSI, MACD, CCI, and membership in the existing portfolio watchlists.",
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
        "candidates": candidates,
        "spotlight": spotlight,
        "worth_watching": worth_watching,
        "stable_watch": stable_names,
        "caution_list": caution_list,
        "model_notes": model_notes,
        "filters": {
            "labels": ["Worth Watching", "Stable", "Caution"],
            "membership": ["Portfolio", "Growth", "Unassigned"],
        },
        "empty_state": len(enriched) == 0,
    }


def write_json_output(payload: dict[str, Any], output_path: Path) -> Path:
    # บันทึก snapshot ที่เตรียมไว้แล้ว เพื่อให้ dashboard อ่าน JSON ที่คงที่แทนการคำนวณใหม่ทุก request
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path


def load_or_build_market_dashboard(output_path: Path | None = None) -> dict[str, Any]:
    # ถ้ามี JSON snapshot ที่เตรียมไว้แล้ว ให้โหลดมาใช้ก่อน
    # ถ้ายังไม่มี ค่อย fallback ไปสร้างใหม่ เพื่อให้การพัฒนา local ยังใช้งานได้
    output_path = output_path or DEFAULT_MARKET_DASHBOARD_OUTPUT
    if output_path.exists():
        try:
            return json.loads(output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return build_market_dashboard_dataset()


def load_or_build_investment_insights(output_path: Path | None = None) -> dict[str, Any]:
    # ใช้แนวคิด lazy-load แบบเดียวกันกับหน้า insights
    output_path = output_path or DEFAULT_INVESTMENT_INSIGHTS_OUTPUT
    if output_path.exists():
        try:
            return json.loads(output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return build_investment_insights_dataset()
