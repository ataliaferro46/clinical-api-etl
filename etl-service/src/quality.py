# etl-service/src/quality.py
import re
from typing import Tuple, Dict, Any, List

BP_RE = re.compile(r"^\s*(\d{2,3})\s*/\s*(\d{2,3})\s*$")

# Canonical units we want to land with
CANONICAL_UNITS = {
    "glucose": "mg/dL",
    "cholesterol": "mg/dL",
    "weight": "kg",
    "height": "cm",
    "heart_rate": "bpm",
    "blood_pressure": "mmHg",
}

# Basic physiological ranges (illustrative, adjustable)
RANGES = {
    "glucose": (30, 500),            # mg/dL
    "cholesterol": (50, 400),        # mg/dL
    "weight": (20, 400),             # kg
    "height": (50, 250),             # cm
    "heart_rate": (30, 230),         # bpm
    "systolic": (60, 260),           # mmHg
    "diastolic": (40, 160),          # mmHg
}

def convert_to_canonical(measurement_type: str, value: str, unit: str) -> Tuple[bool, Dict[str, Any], str | None]:
    """
    Returns (ok, payload, err)
    payload contains:
      - for numeric types: {"value_numeric": float, "unit": <canonical>}
      - for BP: {"systolic": int, "diastolic": int, "unit": "mmHg"}
    """
    mt = measurement_type.lower().strip()
    unit = unit.strip()

    # blood pressure is special (two values in one string)
    if mt == "blood_pressure":
        m = BP_RE.match(value)
        if not m:
            return False, {}, "invalid_bp_format"
        sys, dia = int(m.group(1)), int(m.group(2))
        return True, {"systolic": sys, "diastolic": dia, "unit": "mmHg"}, None

    # everything else: numeric
    try:
        val = float(value)
    except Exception:
        return False, {}, "non_numeric_value"

    # unit conversions (extend as needed)
    # weight: lbs -> kg
    if mt == "weight" and unit.lower() in {"lb", "lbs"}:
        val = val * 0.453592
        unit = "kg"
    # height: inches -> cm
    if mt == "height" and unit.lower() in {"in", "inch", "inches"}:
        val = val * 2.54
        unit = "cm"

    canonical = CANONICAL_UNITS.get(mt)
    # if we have a canonical and we didn't convert into it, enforce it
    if canonical and unit != canonical:
        return False, {}, f"unexpected_unit:{unit}"

    return True, {"value_numeric": val, "unit": canonical or unit}, None

def range_flags(payload: Dict[str, Any]) -> List[str]:
    flags: List[str] = []
    if "systolic" in payload and "diastolic" in payload:
        s_lo, s_hi = RANGES["systolic"]
        d_lo, d_hi = RANGES["diastolic"]
        if not (s_lo <= payload["systolic"] <= s_hi):
            flags.append("systolic_out_of_range")
        if not (d_lo <= payload["diastolic"] <= d_hi):
            flags.append("diastolic_out_of_range")
        return flags

    if "value_numeric" in payload:
        # We could carry the measurement_type in payload to check exact range.
        # For a simple pass, skip; or you can add per-type checks similarly.
        pass
    return flags
