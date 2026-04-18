from web.portfolio_service import build_dashboard_payload, build_growth_dashboard_payload


def test_dashboard_payload_shape() -> None:
    # smoke test สำหรับ payload ของหน้า market dashboard
    # ใช้เช็กว่า key หลักที่ template เว็บต้องใช้ยังคงมีอยู่ครบเสมอ
    payload = build_dashboard_payload()
    assert "generated_at" in payload
    assert "summary" in payload
    assert "securities" in payload
    assert "heatmap" in payload
    assert "scatter" in payload
    assert isinstance(payload["securities"], list)
    assert isinstance(payload["summary"], dict)


def test_growth_dashboard_payload_shape() -> None:
    # smoke test สำหรับ payload ของหน้า investment insights
    # ช่วยจับกรณีที่โครงสร้าง JSON เปลี่ยนจนหน้า dashboard อาจพัง
    payload = build_growth_dashboard_payload()
    assert "generated_at" in payload
    assert "summary" in payload
    assert "spotlight" in payload
    assert "score_distribution" in payload
    assert isinstance(payload["spotlight"], list)
    assert isinstance(payload["summary"], dict)
