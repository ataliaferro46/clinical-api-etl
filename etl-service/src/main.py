# etl-service/src/main.py
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from uuid import uuid4
import os, csv, re, asyncpg
from datetime import datetime

app = FastAPI(title="Clinical Data ETL Service", version="1.0.0")

# In-memory job store (demo-grade; a real system would persist this)
jobs: Dict[str, Dict[str, Any]] = {}

class ETLJobRequest(BaseModel):
    jobId: Optional[str] = None
    filename: str
    studyId: Optional[str] = None

class ETLJobResponse(BaseModel):
    jobId: str
    status: str
    message: str

class ETLJobStatus(BaseModel):
    jobId: str
    status: str
    progress: Optional[int] = 0
    message: Optional[str] = None

DATA_DIR = os.getenv("DATA_DIR", "/data")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_DB   = os.getenv("POSTGRES_DB", "clinical_data")
PG_USER = os.getenv("POSTGRES_USER", "user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "pass")  # matches your docker-compose
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

BP_RE = re.compile(r"^\s*(\d{2,3})\s*/\s*(\d{2,3})\s*$")

def parse_ts(s: Optional[str]):
    """Parse ISO8601 strings (including trailing Z) into timezone-aware datetime."""
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)  # tz-aware datetime
    except Exception:
        return None

async def get_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        user=PG_USER, password=PG_PASS, host=PG_HOST, port=PG_PORT, database=PG_DB,
        min_size=1, max_size=10
    )

UPSERT_STUDY = "INSERT INTO dim_study(study_id) VALUES($1) ON CONFLICT DO NOTHING;"
UPSERT_PARTICIPANT = "INSERT INTO dim_participant(study_id, participant_id) VALUES($1,$2) ON CONFLICT DO NOTHING;"
UPSERT_SITE = "INSERT INTO dim_site(site_id) VALUES($1) ON CONFLICT DO NOTHING;"
UPSERT_MEAS_TYPE = "INSERT INTO dim_measurement_type(name) VALUES($1) ON CONFLICT DO NOTHING;"
UPSERT_UNIT = "INSERT INTO dim_unit(name) VALUES($1) ON CONFLICT DO NOTHING;"

# Cast $10 to timestamptz as a defensive measure
INSERT_FACT = """
INSERT INTO fact_measurement(
  study_id, participant_id, site_id, measurement_type_id, unit_id,
  value_numeric, systolic, diastolic, quality_score, ts, source_file, is_valid, quality_flags
)
SELECT
  $1, $2, $3,
  (SELECT id FROM dim_measurement_type WHERE name=$4),
  (SELECT id FROM dim_unit WHERE name=$5),
  $6, $7, $8, $9, $10::timestamptz, $11, $12, $13;
"""

def parse_row(raw: Dict[str, str]) -> Dict[str, Any]:
    mt = (raw.get("measurement_type") or "").strip().lower()
    unit = (raw.get("unit") or "").strip()
    val = raw.get("value")
    systolic = diastolic = None
    value_numeric: Optional[float] = None
    is_valid = True
    flags: List[str] = []

    if mt == "blood_pressure":
        m = BP_RE.match(val or "")
        if m:
            systolic = int(m.group(1))
            diastolic = int(m.group(2))
            unit = "mmHg"
        else:
            is_valid = False
            flags.append("invalid_bp_format")
    else:
        try:
            value_numeric = float(val) if val is not None else None
        except Exception:
            is_valid = False
            flags.append("non_numeric_value")

    # light unit normalization
    if mt == "weight" and unit.lower() in {"lb", "lbs"} and value_numeric is not None:
        value_numeric *= 0.453592
        unit = "kg"
    if mt == "height" and unit.lower() in {"in", "inch", "inches"} and value_numeric is not None:
        value_numeric *= 2.54
        unit = "cm"

    return {
        "study_id": (raw.get("study_id") or "").strip(),
        "participant_id": (raw.get("participant_id") or "").strip(),
        "site_id": (raw.get("site_id") or "").strip(),
        "measurement_type": mt,
        "unit": unit or "",
        "value_numeric": value_numeric,
        "systolic": systolic,
        "diastolic": diastolic,
        "quality_score": float(raw.get("quality_score") or 0),
        "ts": parse_ts(raw.get("timestamp")),  # tz-aware datetime (or None)
        "flags": flags,
        "is_valid": is_valid,
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "etl"}

@app.post("/jobs", response_model=ETLJobResponse)
async def submit_job(job_request: ETLJobRequest, background_tasks: BackgroundTasks):
    job_id = job_request.jobId or str(uuid4())
    filename = job_request.filename
    study_id = job_request.studyId

    jobs[job_id] = {
        "jobId": job_id,
        "filename": filename,
        "studyId": study_id,
        "status": "running",
        "progress": 0,
        "message": "Job started",
        "createdAt": datetime.utcnow().isoformat(),
        "updatedAt": datetime.utcnow().isoformat(),
    }

    background_tasks.add_task(process_file, job_id, filename, study_id)
    return ETLJobResponse(jobId=job_id, status="running", message="Job submitted successfully")

@app.get("/jobs/{job_id}/status", response_model=ETLJobStatus)
async def get_job_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return ETLJobStatus(
        jobId=job_id,
        status=job["status"],
        progress=job.get("progress", 0),
        message=job.get("message"),
    )

@app.get("/jobs/{job_id}")
async def get_job_details(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

async def process_file(job_id: str, filename: str, study_id: Optional[str]):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        jobs[job_id].update(
            status="failed",
            message="file_not_found",
            updatedAt=datetime.utcnow().isoformat(),
        )
        return

    jobs[job_id].update(status="running", message="starting", progress=0, updatedAt=datetime.utcnow().isoformat())
    print(f"[ETL] start job_id={job_id} file={filename} study={study_id}", flush=True)

    try:
        total = max(0, sum(1 for _ in open(path)) - 1)
    except Exception:
        total = 0
    processed = 0

    pool = await get_pool()
    try:
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            async with pool.acquire() as conn:
                for row in reader:
                    rec = parse_row(row)
                    if study_id:
                        rec["study_id"] = study_id

                    try:
                        async with conn.transaction():
                            await conn.execute(UPSERT_STUDY, rec["study_id"])
                            await conn.execute(UPSERT_PARTICIPANT, rec["study_id"], rec["participant_id"])
                            await conn.execute(UPSERT_SITE, rec["site_id"])
                            await conn.execute(UPSERT_MEAS_TYPE, rec["measurement_type"])
                            await conn.execute(UPSERT_UNIT, rec["unit"])

                            # (Optional) debug:
                            # print(f"TS TYPE={type(rec['ts'])} VAL={rec['ts']}", flush=True)

                            await conn.execute(
                                INSERT_FACT,
                                rec["study_id"], rec["participant_id"], rec["site_id"],
                                rec["measurement_type"], rec["unit"],
                                rec["value_numeric"], rec["systolic"], rec["diastolic"],
                                rec["quality_score"], rec["ts"], filename, rec["is_valid"],
                                rec["flags"] if rec["flags"] else [],
                            )
                    except Exception as e:
                        print(f"[ETL] row error: {e}", flush=True)

                    processed += 1
                    if total and (processed % 100 == 0 or processed == total):
                        pct = int(processed * 100 / total)
                        jobs[job_id].update(
                            progress=pct,
                            message=f"processed {processed}/{total}",
                            updatedAt=datetime.utcnow().isoformat(),
                        )

        jobs[job_id].update(status="completed", progress=100, message="done", updatedAt=datetime.utcnow().isoformat())
        print(f"[ETL] done job_id={job_id} processed={processed} total={total}", flush=True)
    except Exception as e:
        jobs[job_id].update(status="failed", message=str(e), updatedAt=datetime.utcnow().isoformat())
        print(f"[ETL] failed job_id={job_id}: {e}", flush=True)
    finally:
        await pool.close()
