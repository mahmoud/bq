#!/usr/bin/env bash
set -euo pipefail
# Wait for PG (psycopg2 connect loop; pg_isready is not in python:slim)
uv run python - <<'EOF'
import os, time, psycopg2
url = os.environ.get("TEST_DB_URL", "postgresql://bq:@localhost/bq_test")
for _ in range(60):
    try:
        psycopg2.connect(url).close(); break
    except psycopg2.OperationalError:
        time.sleep(1)
else:
    raise SystemExit("PostgreSQL never became ready")
EOF
uv run coverage erase
uv run coverage run -m pytest "${@:-tests}" -q
uv run coverage combine
uv run coverage report -m | tee coverage-report.txt
uv run coverage xml -o coverage.xml
