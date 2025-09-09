# etl-service/tests/conftest.py
import os
import sys
import tempfile
import shutil
import textwrap
import pytest
from fastapi.testclient import TestClient
from src.main import app
# etl-service/tests/conftest.py (very top)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

@pytest.fixture(scope="session", autouse=True)
def _temp_data_dir():
    # Create a temp /data with a minimal sample CSV the ETL might try to read.
    tmp = tempfile.mkdtemp(prefix="etl_data_")
    csv_path = os.path.join(tmp, "sample_study001.csv")
    with open(csv_path, "w") as f:
        f.write(textwrap.dedent("""\
            study_id,participant_id,measurement_type,value,unit,timestamp,site_id,quality_score
            STUDY001,P001,glucose,95.5,mg/dL,2024-01-15T09:30:00Z,SITE_A,0.98
        """))
    # Point the ETL to this directory
    os.environ["DATA_DIR"] = tmp
    yield
    shutil.rmtree(tmp, ignore_errors=True)

@pytest.fixture
def client():
    return TestClient(app)
