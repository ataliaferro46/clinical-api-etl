# etl-service/tests/test_submit_status.py
from fastapi.testclient import TestClient
from src.main import app

def test_submit_and_status(client: TestClient):
    payload = {
        "jobId": "pytest-job-1",
        "filename": "sample_study001.csv",
        "studyId": "STUDY001"
    }
    r = client.post("/jobs", json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j["jobId"] == "pytest-job-1"
    assert j["status"] in ("running", "queued")

    # Fetch status
    r2 = client.get("/jobs/pytest-job-1/status")
    assert r2.status_code == 200
    s = r2.json()
    assert s["jobId"] == "pytest-job-1"
    assert s["status"] in ("running", "completed", "failed")
