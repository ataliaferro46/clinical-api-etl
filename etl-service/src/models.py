# etl-service/src/models.py
from enum import Enum
from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime

class Status(str, Enum):
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"

class JobStatus(BaseModel):
    jobId: str
    status: Status
    progress: int = 0
    message: Optional[str] = None

class RowIn(BaseModel):
    study_id: str
    participant_id: str
    measurement_type: str
    value: str                      # raw value from CSV; may be "120/80"
    unit: str
    timestamp: datetime
    site_id: str
    quality_score: float = Field(ge=0.0, le=1.0)

    @validator("measurement_type")
    def lower_mt(cls, v): return v.strip().lower()
    @validator("unit")
    def norm_unit(cls, v): return v.strip()
