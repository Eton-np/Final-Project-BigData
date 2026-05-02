from __future__ import annotations

# script wrapper ขนาดเล็กสำหรับให้ Airflow หรือการรันแบบ local ใช้สร้าง Market Dashboard JSON snapshot

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# ทำให้ import จาก package jobs ได้ถูกต้อง แม้ไฟล์นี้จะถูกรันตรงด้วย python
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.common import (
    DEFAULT_MARKET_DASHBOARD_READY_MARKER,
    DEFAULT_MARKET_DASHBOARD_OUTPUT,
    build_argument_parser,
)
from jobs.dashboard_datasets import build_market_dashboard_dataset, write_airflow_ready_marker, write_json_output


def main() -> None:
    parser = build_argument_parser("build_market_dashboard_data")
    parser.add_argument("--output-file", default=str(DEFAULT_MARKET_DASHBOARD_OUTPUT))
    parser.add_argument("--mark-airflow-run", action="store_true")
    args = parser.parse_args()

    payload = build_market_dashboard_dataset()
    # JSON ที่เขียนออกมาคือ dataset ที่หน้า FastAPI และ browser จะนำไปใช้ต่อ
    output_path = write_json_output(payload, Path(args.output_file).resolve())
    if args.mark_airflow_run:
        write_airflow_ready_marker(DEFAULT_MARKET_DASHBOARD_READY_MARKER, output_path)


if __name__ == "__main__":
    main()
