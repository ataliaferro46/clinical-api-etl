# Clinical Data ETL Pipeline – Design Notes

## Summary

This project delivers a microservices-based ETL pipeline to process clinical trial data into a normalized PostgreSQL schema optimized for analytics.  
Key points:  
- **Schema Design**: Normalized star-schema with `fact_measurement` and supporting `dim_*` tables. Optimized with indexes for common analytical queries (time-series, study rollups, quality filters).  
- **ETL Implementation**: Python FastAPI service handles job submission, background CSV ingestion, validation, transformation, and database loading.  
- **API Layer**: TypeScript/Node.js service coordinates job submission, job status retrieval, and querying processed data.  
- **AI-assisted Development**: AI tools were used to break down tasks, generate initial scaffolding for API/ETL/database code, debug container issues, and refine schema design decisions. Final code and notes reflect collaboration between AI and myself.  

The rest of this document details:  
1. Problem-solving approach  
2. Schema design decisions  
3. Handling of analytical query requirements  
4. AI usage and development workflow  

## Step 1: Goals
The schema and ETL pipeline were designed to support analytical queries such as:
- Highest quality scores per study
- Time-series trends for specific participants
- Site-level aggregations
- Recent 30-day data snapshots
- Participant counts per study
- Derived metrics like average BMI

## Step 2: Schema Normalization
I normalized the schema into **dimension** and **fact** tables.

- **Dimensions**
  - `dim_study`: canonical study IDs
  - `dim_site`: site identifiers
  - `dim_participant`: composite key `(study_id, participant_id)`
  - `dim_measurement_type`: surrogate keys for measurement types
  - `dim_unit`: surrogate keys for units of measure

- **Fact**
  - `fact_measurement`: contains all measurements with foreign keys to dimension tables
  - Stores numeric values, systolic/diastolic (for blood pressure), quality flags, validity, and timestamp
  - Includes lineage: source file name

- **ETL Jobs**
  - `etl_jobs` tracks submissions, statuses, timestamps

This design eliminates free-text measurement types/units in the fact table, ensuring referential integrity.

---

## Step 3: Indexing Strategy
I created indexes to optimize the most common analytical queries:

- **Participant time series per study**  
  ```sql
  CREATE INDEX idx_fact_study_part_ts
    ON fact_measurement (study_id, participant_id, ts DESC);
  ```

- **Measurement type trends**  
  ```sql
  CREATE INDEX idx_fact_meastype_ts
    ON fact_measurement (measurement_type_id, ts DESC);
  ```

- **Site rollups**  
  ```sql
  CREATE INDEX idx_fact_site
    ON fact_measurement (site_id);
  ```

- **Quality thresholds & invalid-only scans**  
  ```sql
  CREATE INDEX idx_fact_quality_score
    ON fact_measurement (quality_score);

  CREATE INDEX idx_fact_invalid_only
    ON fact_measurement (ts)
    WHERE NOT is_valid;
  ```

- **Searchable flags**  
  ```sql
  CREATE INDEX idx_fact_quality_flags_gin
    ON fact_measurement USING GIN (quality_flags);
  ```

---

- Time-based queries:  
  I created an index combining `measurement_type_id` with `ts DESC` so that time-series queries scoped by measurement type are efficient.  

  ```sql
  CREATE INDEX idx_fact_meastype_ts
    ON fact_measurement (measurement_type_id, ts DESC);
  ```

- Rollups per site:  
  A simple index on `site_id` makes aggregations like “measurements per site” faster.  

  ```sql
  CREATE INDEX idx_fact_site
    ON fact_measurement (site_id);
  ```

- Quality filtering:  
  To support analytics on data quality, I added:
  - An index on `quality_score` for threshold-based filters
  - A partial index on rows where `is_valid = false` to quickly isolate invalid data  

  ```sql
  CREATE INDEX idx_fact_quality_score
    ON fact_measurement (quality_score);

  CREATE INDEX idx_fact_invalid_only
    ON fact_measurement (ts) WHERE NOT is_valid;
  ```

- Searchable quality flags (array):  
  I used a GIN index on the `quality_flags` array to allow efficient searching for specific issues (e.g., rows flagged with `out_of_range`, `bp_parse_error`, etc.).  

  ```sql
  CREATE INDEX idx_fact_quality_flags_gin
    ON fact_measurement USING GIN (quality_flags);
  ```

---

## Step 4: ETL Pipeline
The ETL service was enhanced to:
1. **Parse CSV rows** safely with Pydantic validation  
2. **Normalize values** via helper functions (e.g., blood pressure parsing, unit conversions)  
3. **Upsert into dimension tables**  
4. **Insert into `fact_measurement`** with surrogate keys  
5. **Track job progress and errors** in memory (per assessment spec)  

### Fixes
- Added timestamp parsing (`fromisoformat` with support for `Z` → UTC).  
- Patched loader to cast timestamps correctly to `TIMESTAMPTZ`.  

---

## Step 5: Business Queries Supported
Schema and indexes support the following efficiently:

- **Highest data quality scores per study**  
- **Glucose trends for a participant over time**  
- **Measurement counts across sites**  
- **Filtering by quality threshold**  
- **Last 30 days of collected data**  
- **Participant counts per study**  
- **Aggregates like average BMI per study**  

---


## Step 6: Use of AI Assistance
Throughout the assessment, I used **AI assistants (ChatGPT)** to:
- Break down requirements into actionable steps  
- Draft initial schema and code snippets  
- Debug tricky errors (e.g., timestamp parsing, Docker volume mounts)  
- Suggest indexing strategies  
- Generate explanatory documentation  

I refined AI-generated output manually:
- Verified code behavior through testing  
- Adjusted schema to enforce referential integrity  
- Simplified ETL flow for reliability  
- Rewrote logging to trace issues clearly  

This hybrid approach balanced **speed** with **control and correctness**.  

---

## Step 7: Reflections
- The **normalized schema** prevents data drift and ensures consistent measurement types and units.  
- Indexes directly map to **analytical workloads**.  
- The ETL pipeline is resilient: rows with schema errors are logged as invalid but do not block processing.  
- AI was most valuable for **rapid prototyping**, while I remained responsible for correctness and integration.  

---

# Testing Overview

This project includes tests for both the **API service** (Node/Express + TypeScript) and the **ETL service** (FastAPI + Python). The goal of these tests is to validate core functionality, catch regressions, and provide confidence that services behave correctly.

---

## Why These Tests Exist

- **API Service**
  - **Health check test**: Confirms the `/health` endpoint responds with `200 OK` and expected JSON. This ensures the service is up and routing works.
  - **ETL proxy tests**: Validate that the API properly proxies requests to the ETL service. Scenarios tested:
    - Job exists → returns `200 OK` and correct job data.
    - Job missing → returns `404 Not Found`.
    - ETL service unavailable → returns `500 Internal Server Error`.
  - These cover the most common integration paths and error handling logic.


- **ETL Service**
  - **Health check test**: Confirms the `/health` endpoint responds with `200 OK` and expected JSON.
  - **Job submission test**: Posts a new job with `jobId`, `filename`, and `studyId`, and verifies the response is `200 OK`.
  - **Job status test**: Immediately queries the job status and ensures it reflects the in-memory job tracking (`running`).
  - These tests validate that the ETL service can accept jobs, track them in memory, and expose status consistently.

- Added **edge case tests** for both API and ETL services to validate error handling (e.g., missing fields, invalid job IDs, timeouts, and nonexistent files).  
- These tests ensure robustness by simulating real-world failure scenarios without changing existing production logic.

---

## How to Run the Tests

### API Service (Node/TypeScript)
`bash
cd api-service
npm install
npm test`
### ETL Service (Python/FastAPI)
`cd etl-service
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q`

✅ **Deliverables complete**:
- API job status endpoint (Task 1)  
- ETL pipeline (Task 2)  
- Normalized schema + indexes (Task 3)  
- Documentation of approach (this file)
-Testing of API and ETL services
