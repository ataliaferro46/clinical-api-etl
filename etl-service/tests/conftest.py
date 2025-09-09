import os
import sys
import tempfile
import shutil
import textwrap
import pytest

# ---- Make etl-service/src importable ----
THIS_DIR = os.path.dirname(__file__)
SRC_DIR = os.path.abspath(os.path.join(THIS_DIR, '..', 'src'))
assert os.path.isdir(SRC_DIR), f"src/ not found at {SRC_DIR}"
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
# -----------------------------------------

from fastapi.testclient import TestClient
from main import app 

@pytest.fixture(scope="session", autouse=True)
def _temp_data_dir():
    # Create a temp /data directory with a small CSV
    tmp = tempfile.mkdtemp(prefix="etl_data_")
    csv_path = os.path.join(tmp, "sample_study001.csv")
    with open(csv_path, "w") as f:
        f.write(textwrap.dedent("""\
            study_id,participant_id,measurement_type,value,unit,timestamp,site_id,quality_score
            STUDY001,P001,glucose,95.5,mg/dL,2024-01-15T09:30:00Z,SITE_A,0.98
        """))
    os.environ["DATA_DIR"] = tmp
    try:
        yield
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

@pytest.fixture
def client():
    return TestClient(app)
