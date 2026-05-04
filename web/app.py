from __future__ import annotations

# จุดเริ่มต้นของ FastAPI สำหรับชั้น presentation
# โมดูลนี้ทำหน้าที่เสิร์ฟทั้งหน้า HTML และ JSON API ที่สร้างจาก dashboard payload ที่เตรียมไว้

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from web.portfolio_service import (
    build_investment_insights_payload,
    build_investment_insights_view_payload,
    build_market_dashboard_payload,
    build_market_dashboard_view_payload,
)


APP_ROOT = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(APP_ROOT / "templates"))

app = FastAPI(title="แดชบอร์ดพอร์ตหุ้น")
# static assets จะรวม CSS และ resource ฝั่ง browser ที่ template เรียกใช้
app.mount("/static", StaticFiles(directory=str(APP_ROOT / "static")), name="static")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    # หน้า dashboard หลัก: นำ market overview snapshot มา render ลงใน Jinja template
    payload = build_market_dashboard_view_payload()
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"request": request, "payload": payload},
    )


@app.get("/api/portfolio")
async def get_portfolio() -> dict:
    # JSON endpoint ที่ส่ง market payload ชุดเดียวกันออกไป สำหรับ API หรือการทดสอบ
    return build_market_dashboard_payload()


@app.get("/insights", response_class=HTMLResponse)
async def insights(request: Request) -> HTMLResponse:
    # หน้า dashboard ที่สอง เน้นหุ้นที่ถูกให้คะแนนและจัดเป็น candidate สำหรับลงทุน
    payload = build_investment_insights_view_payload()
    return templates.TemplateResponse(
        request=request,
        name="insights.html",
        context={"request": request, "payload": payload},
    )


@app.get("/growth", response_class=HTMLResponse)
async def growth_alias(request: Request) -> HTMLResponse:
    # alias สำหรับรองรับลิงก์เก่าหรือ demo ที่ยังเรียก /growth อยู่
    return await insights(request)


@app.get("/architecture", response_class=HTMLResponse)
async def architecture(request: Request) -> HTMLResponse:
    # หน้าอธิบาย architecture และ workflow ที่ย่อจาก README สำหรับใช้ประกอบการนำเสนอ
    return templates.TemplateResponse(
        request=request,
        name="architecture.html",
        context={"request": request},
    )


@app.get("/api/investment-insights")
async def get_investment_insights() -> dict:
    # JSON API สำหรับหน้า /insights
    return build_investment_insights_payload()


@app.get("/api/growth-final-picks")
async def get_growth_final_picks() -> dict:
    # ชื่อ API ใหม่ที่ตรงกับ CSV final picks และหน้า /insights
    return build_investment_insights_payload()


@app.get("/api/growth-portfolio")
async def get_legacy_growth_portfolio() -> dict:
    # API alias แบบ legacy ที่คืน payload ชุดเดียวกับหน้า insights
    return build_investment_insights_payload()
