import uuid

def test_submit_missing_filename_returns_422(client):
    job_id = str(uuid.uuid4())
    payload = {"jobId": job_id, "studyId": "STUDY001"}  # no filename
    resp = client.post("/jobs", json=payload)
    # FastAPI/Pydantic should 422 on request model validation error
    assert resp.status_code == 422, resp.text

def test_status_unknown_job_returns_404(client):
    resp = client.get("/jobs/not-a-real-id/status")
    assert resp.status_code == 404

def test_submit_minimal_valid_then_status_ok(client):
    job_id = str(uuid.uuid4())
    payload = {"jobId": job_id, "filename": "sample_study001.csv", "studyId": "STUDY001"}
    r = client.post("/jobs", json=payload)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["jobId"] == job_id
    assert body["status"] in {"running", "queued", "submitted"}

    s = client.get(f"/jobs/{job_id}/status")
    assert s.status_code == 200, s.text
    sb = s.json()
    assert sb["jobId"] == job_id
    assert sb["status"] in {"running", "completed", "failed", "queued"}

def test_submit_with_nonexistent_file_still_accepts_job(client):
    # Current implementation accepts the job (in-memory) regardless of file presence.
    # This asserts that API contract remains stable.
    job_id = str(uuid.uuid4())
    payload = {"jobId": job_id, "filename": "does_not_exist.csv", "studyId": "STUDY001"}
    r = client.post("/jobs", json=payload)
    assert r.status_code == 200
