from __future__ import annotations

import os

import uvicorn


def main() -> None:
    host = os.getenv("DASHBOARD_HOST", "127.0.0.1")
    port = int(os.getenv("DASHBOARD_PORT", "8000"))
    reload_enabled = os.getenv("DASHBOARD_RELOAD", "").strip().lower() in {"1", "true", "yes", "on"}
    uvicorn.run("web.app:app", host=host, port=port, reload=reload_enabled)


if __name__ == "__main__":
    main()
