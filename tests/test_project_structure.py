from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_expected_files_exist() -> None:
    # structural sanity check ของ repository
    # ใช้ยืนยันว่าไฟล์หลักที่เอกสารและ pipeline อ้างถึงยังอยู่ครบ
    expected_files = [
        PROJECT_ROOT / "dags" / "stock_pipeline_dag.py",
        PROJECT_ROOT / "jobs" / "common.py",
        PROJECT_ROOT / "jobs" / "csv_to_parquet.py",
        PROJECT_ROOT / "jobs" / "cleanse_data.py",
        PROJECT_ROOT / "jobs" / "calculate_indicators.py",
        PROJECT_ROOT / "jobs" / "select_portfolio.py",
        PROJECT_ROOT / "jobs" / "export_portfolio.py",
        PROJECT_ROOT / "jobs" / "extract_growth_start_end.py",
        PROJECT_ROOT / "jobs" / "calculate_growth_cagr.py",
        PROJECT_ROOT / "jobs" / "filter_rank_growth.py",
        PROJECT_ROOT / "jobs" / "export_growth_portfolio.py",
        PROJECT_ROOT / "jobs" / "dashboard_datasets.py",
        PROJECT_ROOT / "jobs" / "build_market_dashboard_data.py",
        PROJECT_ROOT / "jobs" / "build_investment_insights.py",
        PROJECT_ROOT / "web" / "app.py",
        PROJECT_ROOT / "web" / "portfolio_service.py",
        PROJECT_ROOT / "web" / "templates" / "index.html",
        PROJECT_ROOT / "web" / "templates" / "growth.html",
        PROJECT_ROOT / "web" / "static" / "styles.css",
        PROJECT_ROOT / "docker-compose.yml",
        PROJECT_ROOT / "requirements.txt",
    ]
    for file_path in expected_files:
        assert file_path.exists(), f"Missing required file: {file_path}"


def test_data_directory_exists() -> None:
    # โฟลเดอร์ข้อมูลหุ้นดิบเป็น input สำคัญของขั้น ingestion จึงต้องมีอยู่จริง
    assert (PROJECT_ROOT / "Data" / "StockHistory").exists()
