from __future__ import annotations

# โมดูลนี้เป็นตัวเชื่อมระหว่างผลลัพธ์จาก data engineering กับหน้า dashboard
# ปัจจุบัน flow หลักใช้ Spark/Parquet ผ่าน dashboard_snapshot_spark.py เพราะเหมาะกับข้อมูลหุ้นจำนวนมาก
# ส่วนโค้ดอ่าน CSV ในไฟล์นี้เก็บไว้เป็น fallback/legacy path สำหรับ debug หรือกรณีที่ Parquet/Spark ยังไม่พร้อม

import csv
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from jobs.common import (
    DEFAULT_EXPORT_OUTPUT,
    DEFAULT_GROWTH_FINAL_PICKS_OUTPUT,
    DEFAULT_INVESTMENT_INSIGHTS_READY_MARKER,
    DEFAULT_INVESTMENT_INSIGHTS_OUTPUT,
    DEFAULT_MARKET_DASHBOARD_READY_MARKER,
    DEFAULT_MARKET_DASHBOARD_OUTPUT,
    PROJECT_ROOT,
)


TODAYS_STOCKS_PATH = PROJECT_ROOT / "Data" / "Todays_stocks.csv"
STOCK_HISTORY_DIR = PROJECT_ROOT / "Data" / "StockHistory"
MAX_REASONABLE_CAGR = 5.0
MIN_LOOKBACK_COVERAGE = 0.8
DEFAULT_GROWTH_SIGNAL_POOL_SIZE = 140


def _use_spark_dashboard_builder() -> bool:
    # จุดตัดสินใจว่าจะใช้ Parquet หรือ CSV:
    # - ค่า default คือ spark จึงใช้ dashboard_snapshot_spark.py ไปอ่าน output/parquet/*
    # - CSV path จะถูกใช้เฉพาะเมื่อ DASHBOARD_DATASET_ENGINE=csv หรือ legacy
    #   และต้องเปิด DASHBOARD_ALLOW_CSV_FALLBACK=1 เพื่อยืนยันว่าอนุญาตให้อ่าน CSV ช้า ๆ ได้
    engine = os.getenv("DASHBOARD_DATASET_ENGINE", "spark").strip().lower()
    fallback_allowed = os.getenv("DASHBOARD_ALLOW_CSV_FALLBACK", "").strip().lower() in {"1", "true", "yes", "y", "on"}
    return engine not in {"csv", "legacy"} or not fallback_allowed


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


def _safe_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _years_before(value: datetime, years: int) -> datetime:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def _scale(value: float, lower: float, upper: float) -> float:
    # helper สำหรับบีบค่า score ให้อยู่ในช่วงที่กำหนด
    return max(lower, min(upper, value))


def _without_history_metrics(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key not in {"returns_by_year", "returns_by_years"}}


def _last_csv_line(path: Path) -> str:
    with path.open("rb") as handle:
        handle.seek(0, 2)
        position = handle.tell()
        buffer = b""
        while position > 0:
            step = min(65536, position)
            position -= step
            handle.seek(position)
            buffer = handle.read(step) + buffer
            lines = buffer.splitlines()
            if len(lines) > 1:
                return lines[-1].decode("utf-8", errors="ignore")
        lines = buffer.decode("utf-8", errors="ignore").splitlines()
        return lines[-1] if lines else ""


def _csv_line_to_dict(header: list[str], line: str) -> dict[str, str]:
    values = next(csv.reader([line]))
    return dict(zip(header, values))


def _first_csv_line_on_or_after(path: Path, header_size: int, cutoff_date: datetime) -> str:
    # CSV ของแต่ละ ticker เรียงตาม Date อยู่แล้ว จึงหาแถวเริ่มต้นของช่วงย้อนหลังด้วย binary search ได้
    file_size = path.stat().st_size
    low = header_size
    high = file_size

    with path.open("rb") as handle:
        while high - low > 65536:
            midpoint = (low + high) // 2
            handle.seek(midpoint)
            handle.readline()
            line = handle.readline().decode("utf-8", errors="ignore")
            row_date = _safe_datetime(line.split(",", 1)[0]) if line else None
            if row_date is None:
                high = midpoint
            elif row_date >= cutoff_date:
                high = midpoint
            else:
                low = handle.tell()

        handle.seek(max(header_size, low - 65536))
        if handle.tell() > header_size:
            handle.readline()
        while True:
            line = handle.readline().decode("utf-8", errors="ignore")
            if not line:
                return ""
            row_date = _safe_datetime(line.split(",", 1)[0])
            if row_date is not None and row_date >= cutoff_date:
                return line.strip()


def _last_csv_line_before(path: Path, header_size: int, cutoff_date: datetime) -> str:
    file_size = path.stat().st_size
    low = header_size
    high = file_size

    with path.open("rb") as handle:
        while high - low > 65536:
            midpoint = (low + high) // 2
            handle.seek(midpoint)
            handle.readline()
            line = handle.readline().decode("utf-8", errors="ignore")
            row_date = _safe_datetime(line.split(",", 1)[0]) if line else None
            if row_date is None:
                high = midpoint
            elif row_date >= cutoff_date:
                high = midpoint
            else:
                low = handle.tell()

        handle.seek(max(header_size, low - 65536))
        if handle.tell() > header_size:
            handle.readline()

        previous_line = ""
        while True:
            line = handle.readline().decode("utf-8", errors="ignore")
            if not line:
                return previous_line.strip()
            row_date = _safe_datetime(line.split(",", 1)[0])
            if row_date is not None and row_date >= cutoff_date:
                return previous_line.strip()
            previous_line = line


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


def _build_returns_by_year(
    csv_path: Path,
    header: list[str],
    header_size: int,
    latest_date: datetime,
    lookback_years: int,
) -> dict[str, Any]:
    returns_by_year: dict[str, Any] = {}
    start_year = latest_date.year - max(lookback_years, 1) + 1
    for year in range(start_year, latest_date.year + 1):
        first_line = _first_csv_line_on_or_after(csv_path, header_size, datetime(year, 1, 1))
        last_line = _last_csv_line_before(csv_path, header_size, datetime(year + 1, 1, 1))
        if not first_line or not last_line:
            continue

        first_row = _csv_line_to_dict(header, first_line)
        last_row = _csv_line_to_dict(header, last_line)
        start_date = _safe_datetime(first_row.get("Date"))
        end_date = _safe_datetime(last_row.get("Date"))
        start_close = _safe_float(first_row.get("Close"))
        end_close = _safe_float(last_row.get("Close"))
        if start_date is None or end_date is None or start_close is None or end_close is None:
            continue
        if start_date.year != year or end_date.year != year:
            continue
        if end_date <= start_date or start_close <= 0 or end_close <= 0:
            continue
        return_pct = (end_close - start_close) / start_close
        if return_pct <= -1.0:
            continue

        range_pct = None
        high_price = _safe_float(last_row.get("High"))
        low_price = _safe_float(last_row.get("Low"))
        if high_price is not None and low_price is not None:
            range_pct = (high_price - low_price) / start_close

        rsi_14 = _safe_float(last_row.get("RSI_14"))
        returns_by_year[str(year)] = {
            "return_pct": return_pct,
            "start_date": first_row.get("Date"),
            "end_date": last_row.get("Date"),
            "start_close": start_close,
            "close": end_close,
            "volume": _safe_float(last_row.get("Volume")) or 0.0,
            "rsi_14": rsi_14,
            "macd_12_26_9": _safe_float(last_row.get("MACD_12_26_9")),
            "cci_14_0_015": _safe_float(last_row.get("CCI_14_0.015")),
            "range_pct": range_pct,
            "rsi_bucket": _rsi_bucket(rsi_14),
        }
    return returns_by_year


def _latest_stock_history_date(stock_history_dir: Path = STOCK_HISTORY_DIR) -> datetime | None:
    # CSV fallback helper: หา latest date จากไฟล์ Data/StockHistory/*.csv
    # ใน production ปกติจะไม่เข้าทางนี้ เพราะ Spark path อ่านข้อมูลนี้จาก Parquet แทน
    latest_date: datetime | None = None
    for csv_path in stock_history_dir.glob("*.csv"):
        last_line = _last_csv_line(csv_path)
        if not last_line or last_line.startswith("Date,"):
            continue
        row_date = _safe_datetime(last_line.split(",", 1)[0])
        if row_date is not None and (latest_date is None or row_date > latest_date):
            latest_date = row_date
    return latest_date


def _load_stock_history_window_rows(
    stock_history_dir: Path = STOCK_HISTORY_DIR,
    latest_date: datetime | None = None,
    tickers: set[str] | None = None,
) -> list[dict[str, Any]]:
    # CSV fallback helper: ใช้ StockHistory เป็น source หลักของหน้าเว็บ โดยสรุปข้อมูลย้อนหลังต่อ ticker
    # หนึ่ง object ต่อหุ้นจะเก็บราคาเริ่มต้นของช่วงย้อนหลัง + ค่าล่าสุด เพื่อไม่ส่ง raw หลายล้านแถวไป browser
    # มีไว้เพื่อให้ยังสร้าง dashboard ได้จาก CSV เมื่อยังไม่มี output/parquet หรือ Spark ใช้งานไม่ได้
    if not stock_history_dir.exists():
        return []

    lookback_years = _safe_int(os.getenv("RAW_INPUT_YEARS_BACK")) or 10
    lookback_options = [1, 3, 5, 10]
    if lookback_years not in lookback_options:
        lookback_options.append(lookback_years)
        lookback_options = sorted(set(lookback_options))
    latest_date = latest_date or _latest_stock_history_date(stock_history_dir)
    if latest_date is None:
        return []

    rows: list[dict[str, Any]] = []
    for csv_path in stock_history_dir.glob("*.csv"):
        ticker = csv_path.stem.strip()
        if tickers is not None and ticker.upper() not in tickers:
            continue
        with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
            header_line = handle.readline()
            header_size = handle.tell()
        if not header_line:
            continue

        header = next(csv.reader([header_line]))
        latest_line = _last_csv_line(csv_path)
        if not latest_line or latest_line.startswith("Date,"):
            continue

        latest_row = _csv_line_to_dict(header, latest_line)
        if _safe_datetime(latest_row.get("Date")) != latest_date:
            continue

        close_price = _safe_float(latest_row.get("Close"))
        if close_price is None or close_price <= 0:
            continue

        returns_by_year = _build_returns_by_year(csv_path, header, header_size, latest_date, lookback_years)
        returns_by_years: dict[str, Any] = {}
        selected_start_row: dict[str, str] | None = None
        for years in lookback_options:
            first_window_line = _first_csv_line_on_or_after(csv_path, header_size, _years_before(latest_date, years))
            if not first_window_line:
                continue
            first_window_row = _csv_line_to_dict(header, first_window_line)
            start_date = _safe_datetime(first_window_row.get("Date"))
            start_close = _safe_float(first_window_row.get("Close"))
            if start_date is None or start_date >= latest_date or start_close is None or start_close <= 0:
                continue
            years_active = (latest_date - start_date).days / 365.25
            if years_active <= 0 or years_active < years * MIN_LOOKBACK_COVERAGE:
                continue
            total_return_pct = (close_price - start_close) / start_close
            return_pct = (close_price / start_close) ** (1.0 / years_active) - 1.0
            if return_pct <= -1.0 or return_pct > MAX_REASONABLE_CAGR:
                continue
            returns_by_years[str(years)] = {
                "return_pct": return_pct,
                "total_return_pct": total_return_pct,
                "start_date": first_window_row.get("Date"),
                "start_close": start_close,
                "years_active": _round(years_active, 2),
            }
            if years == lookback_years:
                selected_start_row = first_window_row

        if not returns_by_years:
            continue
        if selected_start_row is None:
            selected_start_row = _csv_line_to_dict(
                header,
                _first_csv_line_on_or_after(csv_path, header_size, _years_before(latest_date, int(max(returns_by_years, key=int)))),
            )

        raw = {**latest_row}
        raw["Stock"] = ticker
        raw["Open"] = selected_start_row.get("Close") or latest_row.get("Open")
        raw["Date"] = latest_row.get("Date")
        raw["History_Start_Date"] = selected_start_row.get("Date")
        raw["History_Start_Close"] = selected_start_row.get("Close")
        raw["History_Lookback_Years"] = str(lookback_years)
        raw["Returns_By_Years"] = json.dumps(returns_by_years)
        raw["Returns_By_Year"] = json.dumps(returns_by_year)
        raw["Selected_Year"] = str(latest_date.year)
        rows.append(raw)

    return rows


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


def _load_latest_portfolio_rows(portfolio_csv: Path = DEFAULT_EXPORT_OUTPUT) -> list[dict[str, Any]]:
    # CSV fallback helper: อ่าน final_portfolio.csv ที่ export จาก DAG 1
    # ใช้เติม flag ว่าหุ้นตัวไหนอยู่ในพอร์ตหลัก เมื่อไม่ได้ใช้ Parquet lookup จาก dashboard_snapshot_spark.py
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


def _load_growth_rows(growth_csv: Path = DEFAULT_GROWTH_FINAL_PICKS_OUTPUT) -> list[dict[str, Any]]:
    # CSV fallback helper: อ่าน growth final picks ที่ export จาก DAG 2
    # ใช้เช็กว่าหุ้นในตลาดชุดปัจจุบันซ้ำกับ final picks หรือไม่
    return load_growth_final_pick_rows(growth_csv)


def load_growth_final_pick_rows(growth_csv: Path = DEFAULT_GROWTH_FINAL_PICKS_OUTPUT) -> list[dict[str, Any]]:
    # อ่าน Top20_Growth_Final_Picks.csv ให้เป็น payload พร้อมใช้ทั้งใน JSON API และหน้าเว็บ
    if not growth_csv.exists():
        return []

    rows: list[dict[str, Any]] = []
    with growth_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            ticker = (raw.get("Ticker") or "").strip()
            if not ticker:
                continue
            start_price = _safe_float(raw.get("Start_Price"))
            end_price = _safe_float(raw.get("End_Price"))
            return_multiple = end_price / start_price if start_price and start_price > 0 and end_price else None
            rows.append(
                {
                    "final_rank": _safe_int(raw.get("Final_Rank")),
                    "ticker": ticker,
                    "start_date": raw.get("Start_Date"),
                    "start_price": start_price,
                    "end_date": raw.get("End_Date"),
                    "end_price": end_price,
                    "years_active": _safe_float(raw.get("Years_Active")) or 0.0,
                    "cagr_percentage": _safe_float(raw.get("CAGR_Percentage")) or 0.0,
                    "growth_rank": _safe_int(raw.get("Growth_Rank")) or 0,
                    "return_multiple": return_multiple,
                    "total_return_pct": return_multiple - 1 if return_multiple is not None else None,
                    "decision_score": _safe_float(raw.get("Decision_Score")),
                    "insight_score": _safe_float(raw.get("Insight_Score")),
                    "insight_label": raw.get("Insight_Label"),
                    "recommendation": raw.get("Recommendation"),
                    "recommendation_key": raw.get("Recommendation_Key"),
                    "signal": raw.get("Signal"),
                }
            )
    return sorted(rows, key=lambda row: (row.get("final_rank") or 999999, row["growth_rank"] or 999999, row["ticker"]))


def build_growth_final_picks_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    cagr_values = [row.get("cagr_percentage") for row in rows if row.get("cagr_percentage") is not None]
    years_values = [row.get("years_active") for row in rows if row.get("years_active") is not None]
    best_row = rows[0] if rows else {}
    return {
        "count": len(rows),
        "best_ticker": best_row.get("ticker"),
        "best_cagr": best_row.get("cagr_percentage"),
        "best_return_multiple": best_row.get("return_multiple"),
        "average_cagr": _round(sum(cagr_values) / len(cagr_values) if cagr_values else 0.0, 4),
        "median_years_active": _round(median(years_values) if years_values else 0.0, 2),
    }

def _load_today_market_rows(todays_csv: Path = TODAYS_STOCKS_PATH) -> list[dict[str, Any]]:
    # CSV fallback helper: อ่าน Data/Todays_stocks.csv เป็น universe หุ้นปัจจุบัน
    # ทาง Spark/Parquet จะสร้าง universe จาก output/parquet/stocks_indicators หรือ dataset Parquet ที่พร้อมที่สุดแทน
    if not todays_csv.exists():
        return []

    rows: list[dict[str, Any]] = []
    with todays_csv.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            ticker = (raw.get("Stock") or raw.get("Ticker") or "").strip()
            close_price = _safe_float(raw.get("Close"))
            if not ticker or close_price is None or close_price <= 0:
                continue
            rows.append(raw)
    return rows


def _latest_date_from_rows(rows: list[dict[str, Any]]) -> datetime | None:
    latest_date: datetime | None = None
    for row in rows:
        row_date = _safe_datetime(row.get("Date"))
        if row_date is not None and (latest_date is None or row_date > latest_date):
            latest_date = row_date
    return latest_date


def load_market_universe(todays_csv: Path = TODAYS_STOCKS_PATH) -> list[dict[str, Any]]:
    # CSV fallback path: ประกอบ market universe พื้นฐานของ dashboard จาก 3 แหล่ง
    # 1) snapshot ตลาดวันนี้ เป็น universe หลักเพื่อให้ครอบคลุมหุ้น 5000+ ตัว
    # 2) StockHistory บางส่วน เติม annual return ย้อนหลัง 10 ปีให้ ticker ที่มีประวัติ
    # 3) portfolio/growth export ล่าสุด เติม membership flag สำหรับ filter
    # ถ้าใช้ env default ฟังก์ชันนี้จะไม่ถูกเรียก เพราะ build_*_dataset จะ return ไป Spark/Parquet ก่อน
    portfolio_rows = _load_latest_portfolio_rows()
    growth_rows = _load_growth_rows()
    portfolio_lookup = {row["ticker"]: row for row in portfolio_rows}
    growth_lookup = {row["ticker"]: row for row in growth_rows}

    raw_rows = _load_today_market_rows(todays_csv)
    today_tickers = {
        (row.get("Stock") or row.get("Ticker") or "").strip().upper()
        for row in raw_rows
        if (row.get("Stock") or row.get("Ticker") or "").strip()
    }
    history_max_tickers = _safe_int(os.getenv("DASHBOARD_HISTORY_MAX_TICKERS")) or 300
    history_tickers = today_tickers
    if history_max_tickers > 0:
        history_tickers = set(sorted(today_tickers)[:history_max_tickers])
    history_rows = _load_stock_history_window_rows(
        latest_date=_latest_date_from_rows(raw_rows),
        tickers=history_tickers or None,
    )
    history_lookup = {(row.get("Stock") or "").strip().upper(): row for row in history_rows}
    if not raw_rows:
        raw_rows = history_rows

    rows: list[dict[str, Any]] = []
    for today_raw in raw_rows:
        ticker = (today_raw.get("Stock") or today_raw.get("Ticker") or "").strip()
        if not ticker:
            continue

        history_raw = history_lookup.get(ticker.upper())
        raw = {**today_raw}
        if history_raw:
            raw.update(
                {
                    "History_Start_Date": history_raw.get("History_Start_Date"),
                    "History_Start_Close": history_raw.get("History_Start_Close"),
                    "History_Lookback_Years": history_raw.get("History_Lookback_Years"),
                    "Returns_By_Years": history_raw.get("Returns_By_Years"),
                    "Returns_By_Year": history_raw.get("Returns_By_Year"),
                    "Selected_Year": history_raw.get("Selected_Year"),
                }
            )

        open_price = _safe_float(raw.get("Open"))
        close_price = _safe_float(raw.get("Close"))
        high_price = _safe_float(raw.get("High"))
        low_price = _safe_float(raw.get("Low"))
        volume = _safe_float(raw.get("Volume")) or 0.0
        rsi_14 = _safe_float(raw.get("RSI_14"))
        macd = _safe_float(raw.get("MACD_12_26_9"))
        cci = _safe_float(raw.get("CCI_14_0.015"))

        return_pct = None
        total_return_pct = None
        years_active = None
        if open_price is not None and open_price > 0 and close_price is not None and close_price > 0:
            start_date = _safe_datetime(raw.get("History_Start_Date"))
            end_date = _safe_datetime(raw.get("Date"))
            total_return_pct = (close_price - open_price) / open_price
            if start_date is not None and end_date is not None and end_date > start_date:
                years_active = (end_date - start_date).days / 365.25
            if years_active is not None and years_active > 0:
                return_pct = (close_price / open_price) ** (1.0 / years_active) - 1.0
        returns_by_years = json.loads(raw.get("Returns_By_Years") or "{}")
        returns_by_year = json.loads(raw.get("Returns_By_Year") or "{}")
        selected_metric = returns_by_years.get(str(raw.get("History_Lookback_Years") or "10"))
        if selected_metric:
            return_pct = selected_metric.get("return_pct")
            total_return_pct = selected_metric.get("total_return_pct")
            years_active = selected_metric.get("years_active")
        else:
            return_pct = None
            total_return_pct = None
            years_active = None

        range_pct = None
        if open_price is not None and open_price > 0 and high_price is not None and low_price is not None:
            range_pct = (high_price - low_price) / open_price

        portfolio_row = portfolio_lookup.get(ticker)
        growth_row = growth_lookup.get(ticker)

        # แต่ละแถวจะถูกแปลงให้เป็น object ที่มีข้อมูลพร้อมใช้บน dashboard
        # รวมถึง label ที่คำนวณเพิ่ม และ flag สำหรับบอกว่าอยู่ใน portfolio/growth หรือไม่
        row = {
            "ticker": ticker,
            "date": raw.get("Date"),
            "history_start_date": raw.get("History_Start_Date"),
            "history_start_close": _safe_float(raw.get("History_Start_Close")),
            "history_lookback_years": _safe_int(raw.get("History_Lookback_Years")) or 10,
            "selected_year": _safe_int(raw.get("Selected_Year")) or 0,
            "history_years_active": _round(years_active, 2),
            "total_return_pct": total_return_pct,
            "returns_by_year": returns_by_year,
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
    if _use_spark_dashboard_builder():
        # Production/default path: ใช้ Spark อ่าน Parquet โดยตรง
        # ไฟล์ปลายทางที่เกี่ยวข้องอยู่ใน output/parquet/stocks_indicators,
        # output/parquet/portfolio และ output/parquet/growth_ranked
        from jobs.dashboard_snapshot_spark import build_market_dashboard_dataset_spark

        return build_market_dashboard_dataset_spark()

    # CSV fallback path: ใช้เฉพาะเมื่อ env อนุญาตให้อ่าน CSV โดยตรง
    # เหมาะกับการ debug หรือ demo ขนาดเล็ก แต่ไม่ใช่ทางหลักของ DAG 3
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


def _insight_score(row: dict[str, Any]) -> tuple[float, list[str], list[str]]:
    # โมเดลให้คะแนนสำหรับ radar และ growth final picks บนหน้า /insights
    # ผสมทั้ง momentum, risk, technical indicators และการอยู่ใน watchlist เข้าด้วยกัน
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


def _growth_recommendation(insight_label: str | None, insight_score: float | None, warning_count: int) -> tuple[str, str]:
    if insight_label == "Worth Watching" and (insight_score or 0) >= 75 and warning_count <= 1:
        return "consider_first", "Consider First"
    if insight_label == "Worth Watching":
        return "watch_risk", "High Growth, Watch Risk"
    if insight_label == "Stable":
        return "watch_timing", "Watch Timing"
    if insight_label == "Caution":
        return "wait_setup", "Wait for Setup"
    return "needs_review", "Needs Review"


def enrich_growth_final_pick_rows(
    growth_final_picks: list[dict[str, Any]],
    market_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    market_lookup = {row["ticker"]: row for row in market_rows if row.get("ticker")}
    enriched_rows: list[dict[str, Any]] = []
    for row in growth_final_picks:
        market_row = market_lookup.get(row["ticker"])
        if not market_row:
            enriched_rows.append(
                {**row, "recommendation_key": "needs_review", "recommendation": "Needs Review", "decision_score": None}
            )
            continue

        score, reasons, warnings = _insight_score(market_row)
        label = _insight_label(score)
        growth_pool_size = _safe_int(os.getenv("GROWTH_SIGNAL_POOL_SIZE")) or DEFAULT_GROWTH_SIGNAL_POOL_SIZE
        rank_score = max(0.0, ((growth_pool_size + 1) - (row.get("growth_rank") or growth_pool_size + 1)) / growth_pool_size) * 100
        decision_score = _scale((score * 0.65) + (rank_score * 0.35) - (len(warnings) * 4), 0, 100)
        recommendation_key, recommendation = _growth_recommendation(label, score, len(warnings))
        enriched_rows.append(
            {
                **row,
                "insight_score": round(score, 2),
                "insight_label": label,
                "reasons": reasons,
                "warnings": warnings,
                "signal": market_row.get("signal"),
                "rsi_14": market_row.get("rsi_14"),
                "macd_12_26_9": market_row.get("macd_12_26_9"),
                "range_pct": market_row.get("range_pct"),
                "recommendation_key": recommendation_key,
                "recommendation": recommendation,
                "decision_score": round(decision_score, 2),
            }
        )
    return sorted(enriched_rows, key=lambda item: (-(item.get("decision_score") or 0), item.get("growth_rank") or 999999))

def build_investment_insights_dataset() -> dict[str, Any]:
    if _use_spark_dashboard_builder():
        # Production/default path: ใช้ Spark อ่าน Parquet แล้วสร้าง insight payload
        # ใช้ข้อมูลเดียวกับ market dashboard และเติมสถานะจาก portfolio/growth Parquet lookup
        from jobs.dashboard_snapshot_spark import build_investment_insights_dataset_spark

        return build_investment_insights_dataset_spark()

    # CSV fallback path: ใช้ market universe ที่ประกอบจาก CSV แล้วคำนวณ insight score ใน Python
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

    # ตัดชุดข้อมูลย่อยไว้ล่วงหน้า เพื่อให้แต่ละ section ของ UI ใช้งานได้ทันทีโดยไม่ต้องจัดอันดับใหม่ใน browser
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


def write_json_output(payload: dict[str, Any], output_path: Path) -> Path:
    # บันทึก snapshot ที่เตรียมไว้แล้ว เพื่อให้ dashboard อ่าน JSON ที่คงที่แทนการคำนวณใหม่ทุก request
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path


def write_airflow_ready_marker(marker_path: Path, snapshot_path: Path) -> Path:
    # marker นี้เป็นหลักฐานว่า snapshot ถูก refresh จาก Airflow task แล้ว
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(
        json.dumps(
            {
                "snapshot_path": str(snapshot_path),
                "marked_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return marker_path


def _marker_is_ready_for_session(marker_path: Path, min_ready_time: datetime | None) -> bool:
    if not marker_path.exists():
        return False
    if min_ready_time is None:
        return True
    marker_mtime = datetime.fromtimestamp(marker_path.stat().st_mtime, tz=timezone.utc)
    return marker_mtime >= min_ready_time


def load_or_build_market_dashboard(
    output_path: Path | None = None,
    min_ready_time: datetime | None = None,
) -> dict[str, Any]:
    # หน้าเว็บต้องอ่านเฉพาะ snapshot ที่ Airflow task เตรียมไว้แล้วเท่านั้น
    # ถ้ายังไม่มี marker จาก Airflow หรือไฟล์เสีย ให้คืน payload ว่าง
    output_path = output_path or DEFAULT_MARKET_DASHBOARD_OUTPUT
    marker_path = DEFAULT_MARKET_DASHBOARD_READY_MARKER
    if _marker_is_ready_for_session(marker_path, min_ready_time) and output_path.exists():
        try:
            return json.loads(output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {
        "generated_at": None,
        "data_as_of": None,
        "summary": {
            "tracked_count": 0,
            "advancers": 0,
            "decliners": 0,
            "unchanged": 0,
            "average_return": 0.0,
            "median_rsi": 0.0,
            "average_range_pct": 0.0,
            "total_volume": 0,
            "breadth_ratio": 0.0,
            "portfolio_overlap": 0,
            "growth_overlap": 0,
            "pipeline_lookback_years": 0,
            "selected_year": 0,
        },
        "highlights": {
            "strongest_momentum": None,
            "highest_turnover": None,
            "deepest_pullback": None,
            "broadest_theme": None,
        },
        "signals": [],
        "price_bands": [],
        "rsi_buckets": [],
        "top_gainers": [],
        "top_losers": [],
        "most_active": [],
        "heatmap": [],
        "scatter": [],
        "securities": [],
        "filters": {
            "signals": [],
            "price_bands": [],
            "rsi_buckets": [],
            "lookback_years": [1, 3, 5, 10],
            "years": [],
        },
        "empty_state": True,
    }


def load_or_build_investment_insights(
    output_path: Path | None = None,
    min_ready_time: datetime | None = None,
) -> dict[str, Any]:
    # ใช้ snapshot ที่ Airflow task เขียนไว้เท่านั้น ถ้ายังไม่มี marker ให้หน้าเว็บแสดงข้อมูลว่าง
    output_path = output_path or DEFAULT_INVESTMENT_INSIGHTS_OUTPUT
    marker_path = DEFAULT_INVESTMENT_INSIGHTS_READY_MARKER
    if _marker_is_ready_for_session(marker_path, min_ready_time) and output_path.exists():
        try:
            return json.loads(output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {
        "generated_at": None,
        "data_as_of": None,
        "summary": {
            "tracked_count": 0,
            "worth_watching": 0,
            "stable": 0,
            "caution": 0,
            "average_score": 0.0,
            "top_score": None,
            "portfolio_overlap": 0,
            "growth_overlap": 0,
            "pipeline_lookback_years": 0,
        },
        "score_distribution": [
            {"label": "Worth Watching", "count": 0},
            {"label": "Stable", "count": 0},
            {"label": "Caution", "count": 0},
        ],
        "candidates": [],
        "spotlight": [],
        "growth_final_picks": [],
        "growth_final_picks_summary": build_growth_final_picks_summary([]),
        "worth_watching": [],
        "stable_watch": [],
        "caution_list": [],
        "model_notes": [],
        "filters": {
            "labels": ["Worth Watching", "Stable", "Caution"],
            "membership": ["Portfolio", "Growth", "Unassigned"],
        },
        "empty_state": True,
    }
