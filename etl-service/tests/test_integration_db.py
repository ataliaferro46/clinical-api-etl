import os
import sys
import time
import uuid
import textwrap
import importlib
from pathlib import Path

import pytest
import psycopg2

TESTS_DIR = Path(__file__).parent
PROJ_ROOT = (TESTS_DIR / "..").resolve()
SRC_DIR = (PROJ_ROOT / "src").resolve()
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

RUN_INTEGRATION = os.getenv("E2E") == "1"


@pytest.mark.skipif(not RUN_INTEGRATION, reason="Set E2E=1 to run DB integration tests")
def test_etl_job_inserts_into_db(tmp_path: Path):
    """
    End-to-end integration:
      1) Create a temp CSV and point DATA_DIR at it
      2) POST /jobs with that filename
      3) Poll /jobs/:id/status until 'completed'
      4) Check Postgres for inserted rows
    Requires: docker compose up (postgres reachable on localhost:5432)
    """

    # 1) Prepare temp data and **set env BEFORE importing app**
    data_dir = tmp_path
    csv_name = "sample_study001.csv"
    (data_dir / csv_name).write_text(
        textwrap.dedent(
            """\
            study_id,participant_id,measurement_type,value,unit,timestamp,site_id,quality_score
            STUDY001,P001,glucose,95.5,mg/dL,2024-01-15T09:30:00Z,SITE_A,0.98
            STUDY001,P001,glucose,100.2,mg/dL,2024-01-15T10:15:00Z,SITE_A,0.97
            """
        )
    )

    # ETL file location
    os.environ["DATA_DIR"] = str(data_dir)

    # Point the ETL’s asyncpg pool at your local Postgres (not 'postgres')
    os.environ["POSTGRES_HOST"] = os.environ.get("POSTGRES_HOST", "localhost")
    os.environ["POSTGRES_PORT"] = os.environ.get("POSTGRES_PORT", "5432")
    os.environ["POSTGRES_DB"] = os.environ.get("POSTGRES_DB", "clinical_data")
    os.environ["POSTGRES_USER"] = os.environ.get("POSTGRES_USER", "user")
    os.environ["POSTGRES_PASSWORD"] = os.environ.get("POSTGRES_PASSWORD", "pass")

    # Optional: if your code supports DATABASE_URL, set it too (harmless if ignored)
    os.environ.setdefault(
        "DATABASE_URL",
        "postgresql://user:pass@localhost:5432/clinical_data",
    )

    # 2) Import app AFTER env is set
    from fastapi.testclient import TestClient
    import main as etl_main
    importlib.reload(etl_main)
    app = etl_main.app
    client = TestClient(app)

    # Submit job
    resp = client.post("/jobs", json={"filename": csv_name, "studyId": "STUDY001"}, timeout=10)
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    etl_job_id = payload.get("jobId") or payload.get("job_id")
    assert etl_job_id, f"Unexpected response payload: {payload}"

    # Poll status
    last = None
    for _ in range(20):
        status_resp = client.get(f"/jobs/{etl_job_id}/status", timeout=10)
        assert status_resp.status_code == 200, status_resp.text
        last = status_resp.json()
        if last.get("status") in ("completed", "failed"):
            break
        time.sleep(1)

    assert last is not None, "No status received"
    assert last.get("status") == "completed", f"ETL did not complete: {last}"

    # 3) Verify DB rows
    dsn = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/clinical_data")
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_measurement WHERE study_id = %s;", ("STUDY001",))
            count = cur.fetchone()[0]
    finally:
        conn.close()

    assert count >= 2, f"Expected >=2 rows for STUDY001, got {count}"
