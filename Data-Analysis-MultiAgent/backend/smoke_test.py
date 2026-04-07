"""Simple smoke tests for non-OAuth API flows.

Usage examples:
  python backend/smoke_test.py
  python backend/smoke_test.py --skip-startup-db
  python backend/smoke_test.py --skip-auth
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.api import app  # noqa: E402


def _expect(name: str, response: Any, expected_status: int, predicate) -> tuple[bool, str]:
    ok = response.status_code == expected_status and predicate(response)
    if ok:
        return True, f"PASS: {name} -> {response.status_code}"
    return (
        False,
        f"FAIL: {name} -> expected {expected_status}, got {response.status_code}, body={response.text}",
    )


def run_smoke(skip_startup_db: bool, skip_auth: bool) -> int:
    if skip_startup_db:
        # Useful on machines where Postgres is not running.
        app.router.on_startup.clear()

    checks: list[tuple[bool, str]] = []

    with TestClient(app) as client:
        health = client.get("/health")
        checks.append(_expect("GET /health", health, 200, lambda r: r.json().get("status") == "ok"))

        if not skip_auth:
            register = client.post(
                "/auth/register",
                json={"email": "smoke.user@example.com", "password": "secret123"},
            )
            checks.append((register.status_code == 200, f"INFO: POST /auth/register -> {register.status_code}"))

            login = client.post(
                "/auth/login",
                json={"email": "smoke.user@example.com", "password": "secret123"},
            )
            checks.append((login.status_code == 200, f"INFO: POST /auth/login -> {login.status_code}"))

            reset = client.post(
                "/auth/reset-password",
                json={"email": "smoke.user@example.com", "new_password": "secret456"},
            )
            checks.append((reset.status_code == 200, f"INFO: POST /auth/reset-password -> {reset.status_code}"))

        bad_type = client.post(
            "/analyze",
            params={"user_id": 1},
            files={"file": ("not_csv.txt", b"a,b\n1,2", "text/plain")},
        )
        checks.append(
            _expect(
                "POST /analyze with non-CSV",
                bad_type,
                404 if skip_startup_db else 400,
                lambda r: True,
            )
        )

    failures = [line for ok, line in checks if not ok]
    for _, line in checks:
        print(line)

    if failures:
        print("\nSmoke test completed with failures.")
        return 1

    print("\nSmoke test completed successfully.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run backend smoke tests (excluding OAuth routes)")
    parser.add_argument(
        "--skip-startup-db",
        action="store_true",
        help="Skip app startup hooks (useful when Postgres is not running)",
    )
    parser.add_argument(
        "--skip-auth",
        action="store_true",
        help="Skip non-OAuth auth endpoint checks",
    )
    args = parser.parse_args()
    return run_smoke(skip_startup_db=args.skip_startup_db, skip_auth=args.skip_auth)


if __name__ == "__main__":
    raise SystemExit(main())
