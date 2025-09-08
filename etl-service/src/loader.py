from asyncpg import Pool
from typing import Dict, Any, List

UPSERT_STUDY = """
INSERT INTO dim_study(study_id) VALUES($1)
ON CONFLICT (study_id) DO NOTHING;
"""

UPSERT_PARTICIPANT = """
INSERT INTO dim_participant(study_id, participant_id) VALUES($1,$2)
ON CONFLICT (study_id, participant_id) DO NOTHING;
"""

UPSERT_SITE = """
INSERT INTO dim_site(site_id) VALUES($1)
ON CONFLICT (site_id) DO NOTHING;
"""

UPSERT_MEAS_TYPE = """
INSERT INTO dim_measurement_type(name) VALUES($1)
ON CONFLICT (name) DO NOTHING;
"""

UPSERT_UNIT = """
INSERT INTO dim_unit(name) VALUES($1)
ON CONFLICT (name) DO NOTHING;
"""

INSERT_MEAS = """
INSERT INTO fact_measurement(
  study_id, participant_id, site_id, measurement_type_id, unit_id,
  value_numeric, systolic, diastolic, quality_score, ts, source_file, is_valid, quality_flags
)
SELECT
  $1, $2, $3,
  (SELECT id FROM dim_measurement_type WHERE name=$4),
  (SELECT id FROM dim_unit WHERE name=$5),
  $6, $7, $8, $9, $10, $11, $12, $13;
"""

async def load_row(pool: Pool, ri: Dict[str, Any], payload: Dict[str, Any], source_file: str, is_valid: bool, flags: List[str]):
    study_id = ri["study_id"]
    participant_id = ri["participant_id"]
    site_id = ri["site_id"]
    mt = ri["measurement_type"]
    ts = ri["timestamp"]
    q = ri["quality_score"]

    unit = payload.get("unit") or ri.get("unit")  # fallback to raw
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(UPSERT_STUDY, study_id)
            await conn.execute(UPSERT_PARTICIPANT, study_id, participant_id)
            await conn.execute(UPSERT_SITE, site_id)
            await conn.execute(UPSERT_MEAS_TYPE, mt)
            await conn.execute(UPSERT_UNIT, unit)

            await conn.execute(
                INSERT_MEAS,
                study_id,
                participant_id,
                site_id,
                mt,
                unit,
                payload.get("value_numeric"),
                payload.get("systolic"),
                payload.get("diastolic"),
                q,
                ts,
                source_file,
                is_valid,
                flags if flags else [],
            )
