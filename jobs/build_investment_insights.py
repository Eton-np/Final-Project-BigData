from __future__ import annotations

# script wrapper ขนาดเล็กสำหรับให้ Airflow หรือการรันแบบ local ใช้สร้าง Investment Insights JSON snapshot

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# ทำให้ import ภายในโปรเจกต์ทำงานได้ แม้ไฟล์นี้จะถูกรันแบบ standalone
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.common import (
    DEFAULT_INVESTMENT_INSIGHTS_OUTPUT,
    build_argument_parser,
)
from jobs.dashboard_datasets import build_investment_insights_dataset, write_json_output


def main() -> None:
    parser = build_argument_parser("build_investment_insights")
    parser.add_argument("--output-file", default=str(DEFAULT_INVESTMENT_INSIGHTS_OUTPUT))
    args = parser.parse_args()

    payload = build_investment_insights_dataset()
    # JSON ที่เขียนออกมาคือ dataset ที่หน้า /insights และ API ที่เกี่ยวข้องจะนำไปใช้
    write_json_output(payload, Path(args.output_file).resolve())


if __name__ == "__main__":
    main()
