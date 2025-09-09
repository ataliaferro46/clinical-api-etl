# etl-service/tests/test_health.py
from fastapi.testclient import TestClient
from src.main import app

def test_health():
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    j = r.json()
    assert j.get("status") in ("healthy", True, "ok")
