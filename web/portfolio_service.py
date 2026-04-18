from __future__ import annotations

# service layer ขนาดเล็กที่คั่นระหว่าง FastAPI routes กับโมดูลสร้าง dataset
# แยกชั้นนี้ไว้ช่วยให้ route handler สั้นลง และยังคงชื่อ import เดิมที่ test ใช้อยู่ได้

from jobs.dashboard_datasets import (
    load_or_build_investment_insights,
    load_or_build_market_dashboard,
)


MARKET_LABELS = {
    "Momentum": "โมเมนตัมเด่น",
    "Pressure": "แรงขายกดดัน",
    "Balanced": "สมดุล",
    "Watch": "น่าจับตา",
    "Unknown": "ไม่ทราบ",
    "Overbought": "ซื้อมากเกินไป",
    "Strength": "แข็งแกร่ง",
    "Weakening": "เริ่มอ่อนแรง",
    "Oversold": "ขายมากเกินไป",
    "Large Cap Proxy": "ราคาสูง",
    "Mid Price": "ราคาปานกลาง",
    "Active Small Cap": "เล็กแต่ซื้อขายคึกคัก",
    "Speculative": "เชิงเก็งกำไร",
    "Portfolio": "พอร์ตหลัก",
    "Growth": "เฝ้าดูเติบโต",
    "Unassigned": "ทั่วไป",
}

INSIGHT_LABELS = {
    "Worth Watching": "น่าจับตา",
    "Stable": "ค่อนข้างนิ่ง",
    "Caution": "ควรระวัง",
}

INSIGHT_TEXT = {
    "Already selected in the core portfolio": "อยู่ในพอร์ตหลักแล้ว",
    "Also appears in the growth watchlist": "อยู่ในรายชื่อเฝ้าดูหุ้นเติบโตด้วย",
    "Strong intraday move": "ราคาเคลื่อนไหวเด่นระหว่างวัน",
    "Selling pressure is elevated": "แรงขายค่อนข้างสูง",
    "Wide trading range signals higher risk": "ช่วงแกว่งกว้าง สะท้อนความเสี่ยงสูงขึ้น",
    "RSI sits in a healthy momentum zone": "RSI อยู่ในโซนโมเมนตัมที่ดี",
    "RSI is overbought": "RSI อยู่ในภาวะซื้อมากเกินไป",
    "RSI remains weak": "RSI ยังอ่อนแรง",
    "MACD remains above zero": "MACD ยังอยู่เหนือศูนย์",
    "MACD is still negative": "MACD ยังติดลบ",
    "CCI confirms upside acceleration": "CCI ยืนยันแรงเร่งขาขึ้น",
    "CCI shows downside stress": "CCI สะท้อนแรงกดดันขาลง",
    "Low price profile increases speculative risk": "ระดับราคาต่ำเพิ่มความเสี่ยงเชิงเก็งกำไร",
    "Signal Model": "โมเดลสัญญาณ",
    "Interpretation": "วิธีตีความ",
    "Workflow": "การอัปเดตข้อมูล",
    "The insights score blends intraday return, range, RSI, MACD, CCI, and membership in the existing portfolio watchlists.": "คะแนนอินไซต์ผสานผลตอบแทนระหว่างวัน ช่วงแกว่ง RSI, MACD, CCI และสถานะในรายชื่อเฝ้าดูที่มีอยู่เข้าด้วยกัน",
    "Worth Watching names combine better momentum with cleaner risk signals, Stable names look mixed, and Caution names show heavier downside pressure or speculative risk.": "กลุ่มน่าจับตาจะมีโมเมนตัมดีกว่าและสัญญาณความเสี่ยงสะอาดกว่า กลุ่มค่อนข้างนิ่งมีภาพรวมผสมกัน ส่วนกลุ่มควรระวังมักเผชิญแรงกดดันขาลงหรือความเสี่ยงเชิงเก็งกำไรสูงกว่า",
    "Airflow refreshes these JSON outputs so the dashboard reads the same prepared snapshot every time the pipeline completes.": "Airflow จะรีเฟรชไฟล์ JSON เหล่านี้ เพื่อให้แดชบอร์ดอ่าน snapshot ชุดเดียวกันทุกครั้งที่ pipeline ทำงานเสร็จ",
}


def _translate(value: str) -> str:
    if value in MARKET_LABELS:
        return MARKET_LABELS[value]
    if value in INSIGHT_LABELS:
        return INSIGHT_LABELS[value]
    if value in INSIGHT_TEXT:
        return INSIGHT_TEXT[value]
    return value


def _localize_market_payload(payload: dict) -> dict:
    localized = {**payload}
    localized["signals"] = [
        {**item, "label": _translate(item.get("label", ""))}
        for item in payload.get("signals", [])
    ]
    localized["price_bands"] = [
        {**item, "label": _translate(item.get("label", ""))}
        for item in payload.get("price_bands", [])
    ]
    localized["rsi_buckets"] = [
        {**item, "label": _translate(item.get("label", ""))}
        for item in payload.get("rsi_buckets", [])
    ]
    localized["filters"] = {
        **payload.get("filters", {}),
        "signals": [_translate(value) for value in payload.get("filters", {}).get("signals", [])],
        "price_bands": [_translate(value) for value in payload.get("filters", {}).get("price_bands", [])],
        "rsi_buckets": [_translate(value) for value in payload.get("filters", {}).get("rsi_buckets", [])],
    }
    localized["highlights"] = {
        **payload.get("highlights", {}),
        "broadest_theme": _translate(payload.get("highlights", {}).get("broadest_theme")),
    }
    localized["securities"] = [
        {
            **row,
            "signal": _translate(row.get("signal", "")),
            "price_band": _translate(row.get("price_band", "")),
            "rsi_bucket": _translate(row.get("rsi_bucket", "")),
        }
        for row in payload.get("securities", [])
    ]
    return localized


def _localize_insight_payload(payload: dict) -> dict:
    localized = {**payload}
    localized["score_distribution"] = [
        {**item, "label": _translate(item.get("label", ""))}
        for item in payload.get("score_distribution", [])
    ]
    localized["filters"] = {
        **payload.get("filters", {}),
        "labels": [_translate(value) for value in payload.get("filters", {}).get("labels", [])],
        "membership": [_translate(value) for value in payload.get("filters", {}).get("membership", [])],
    }
    localized["model_notes"] = [
        {
            **note,
            "title": _translate(note.get("title", "")),
            "body": _translate(note.get("body", "")),
        }
        for note in payload.get("model_notes", [])
    ]
    localized["candidates"] = [
        {
            **row,
            "signal": _translate(row.get("signal", "")),
            "price_band": _translate(row.get("price_band", "")),
            "rsi_bucket": _translate(row.get("rsi_bucket", "")),
            "insight_label": _translate(row.get("insight_label", "")),
            "reasons": [_translate(value) for value in row.get("reasons", [])],
            "warnings": [_translate(value) for value in row.get("warnings", [])],
        }
        for row in payload.get("candidates", [])
    ]
    return localized


def build_market_dashboard_payload() -> dict:
    # คืนค่า market payload จาก JSON snapshot ที่เตรียมไว้ หรือสร้างใหม่ถ้ายังไม่มี
    return load_or_build_market_dashboard()


def build_investment_insights_payload() -> dict:
    # คืนค่า insights payload จาก JSON snapshot ที่เตรียมไว้ หรือสร้างใหม่ถ้ายังไม่มี
    return load_or_build_investment_insights()


# Backward-compatible names used by the current tests and older imports.
def build_dashboard_payload() -> dict:
    return build_market_dashboard_payload()


def build_growth_dashboard_payload() -> dict:
    return build_investment_insights_payload()


def build_market_dashboard_view_payload() -> dict:
    return _localize_market_payload(build_market_dashboard_payload())


def build_investment_insights_view_payload() -> dict:
    return _localize_insight_payload(build_investment_insights_payload())
