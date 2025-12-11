#!/usr/bin/env python3
import csv
import json
import os
import re
from rapidfuzz import fuzz


# ================= CONFIG =================
CSV_FILENAME = "constructproTwoData.csv"   # <-- set your csv here

COL_PAYLOAD = "Payload"
COL_MANUAL = "Manual Spec"
COL_STATUS = "Scrape Status"
COL_COMPARISON = "Comparison Confirmation"

REQUIRED_COLUMNS = {COL_PAYLOAD, COL_MANUAL, COL_STATUS}


# ================= NORMALIZATION =================

def normalize_value(v: str) -> str:
    """Normalize text for regex and fuzzy comparison."""
    v = (v or "").lower().strip()
    v = v.replace("–", "-")
    v = re.sub(r"\s+", " ", v)

    # Unit normalisation
    v = v.replace(" watt", "w").replace(" w", "w")
    v = v.replace(" lm", "lm")
    v = v.replace(" k", "k")

    return v


def normalize(text: str) -> str:
    """General normalization for manual text."""
    return normalize_value(text)


# ================= SPECIFICATION EXTRACTION =================

def extract_specification(payload_json: dict) -> dict:
    """Return the workflow specification from verified.specification."""
    try:
        return payload_json.get("verified", {}).get("specification", {}) or {}
    except Exception:
        return {}


# ================= ATTRIBUTE MATCHING =================

def compare_attribute(workflow_val: str, manual_text: str):
    """
    Compare a workflow value against manual text.

    Returns:
        (is_match: bool, mismatch_value: str or None)
    """
    workflow_norm = normalize_value(workflow_val)
    manual_norm = normalize_value(manual_text)

    # Direct literal substring / regex match
    if workflow_norm and re.search(re.escape(workflow_norm), manual_norm):
        return True, None

    # Detect numeric-unit mismatch like 10w, 800lm, 2700k
    num_unit_pattern = r"(\d+(\.\d+)?\s*(w|k|lm))"
    workflow_nums = re.findall(num_unit_pattern, workflow_norm)
    manual_nums = re.findall(num_unit_pattern, manual_norm)

    if workflow_nums and manual_nums:
        for w in workflow_nums:
            w_clean = w[0].replace(" ", "")
            for m in manual_nums:
                m_clean = m[0].replace(" ", "")
                if w_clean != m_clean:
                    return False, m_clean

    # Fuzzy match
    if fuzz.partial_ratio(workflow_norm, manual_norm) >= 85:
        return True, None

    return False, None


# ================= MANUAL ATTRIBUTE EXTRACTION (for missing detection) =================

def extract_manual_attributes(manual_text: str) -> dict:
    """
    Detect attributes mentioned in manual text.
    This is used to find attributes that appear in manual but NOT in workflow.
    """
    manual = normalize_value(manual_text)

    patterns = {
        "brand": r"\bbrand[: ]+([a-z0-9\s]+)",
        "wattage": r"(\d+(\.\d+)?w)",
        "lumens": r"(\d+lm)",
        "colour temperature": r"(\d{3,4}k|warm white|cool white|daylight)",
        "cap fitting": r"\b(b22|e27|e14)\b",
        "dimmable": r"\bdimmable\b",
        "guarantee": r"(\d+\s*year)",
    }

    detected = {}
    for attr_name, pattern in patterns.items():
        m = re.search(pattern, manual)
        if m:
            detected[attr_name] = m.group(0)

    return detected


# ================= MAIN COMPARISON ENGINE =================

def compare(manual_text: str, payload_json: dict) -> str:
    """Full specification comparison using workflow values → manual text."""
    manual = normalize(manual_text)
    spec = extract_specification(payload_json)

    matched = []
    mismatched = []
    missing = []

    # ---- 1. Workflow → Manual comparison ----
    for key, val in spec.items():
        key_norm = key.lower().strip()
        val_norm = normalize_value(val)

        if not val_norm:
            continue

        is_match, mismatch_value = compare_attribute(val_norm, manual)

        if is_match:
            matched.append(f"{key}: {val}")
        else:
            if mismatch_value:
                mismatched.append(
                    f"{key}\n"
                    f"  Manual: {mismatch_value}\n"
                    f"  Workflow: {val_norm}"
                )
            else:
                missing.append(
                    f"{key}\n"
                    f"  Manual: Not detected\n"
                    f"  Workflow: {val_norm}"
                )

    # ---- 2. Manual → Workflow (missing workflow attributes) ----
    manual_attrs = extract_manual_attributes(manual_text)

    for attr_name, manual_value in manual_attrs.items():
        if not any(attr_name in k.lower() for k in spec.keys()):
            missing.append(
                f"{attr_name.title()}\n"
                f"  Manual: {manual_value}\n"
                f"  Workflow: Not found"
            )

    # ---- Build Response ----
    parts = []

    if matched:
        parts.append("✅ Matched:\n- " + "\n- ".join(matched))
    if mismatched:
        parts.append("⚠️ Mismatched:\n- " + "\n- ".join(mismatched))
    if missing:
        parts.append("❌ Missing:\n- " + "\n- ".join(missing))

    return "\n\n".join(parts) if parts else "✅ No specifications detected"


# ================= CSV HANDLING =================

def find_csv():
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(here, CSV_FILENAME)
    if not os.path.isfile(path):
        raise RuntimeError(f"CSV file '{CSV_FILENAME}' not found")
    return path


def process():
    path = find_csv()
    out = path.replace(".csv", "_with_comparison.csv")

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not REQUIRED_COLUMNS.issubset(reader.fieldnames):
            raise RuntimeError("Missing required columns")

        rows = []
        for row in reader:
            if row.get(COL_STATUS, "").strip().lower() != "success":
                row[COL_COMPARISON] = "Not Applicable"
                rows.append(row)
                continue

            try:
                payload = json.loads(row[COL_PAYLOAD])
            except Exception:
                row[COL_COMPARISON] = "Not Applicable"
                rows.append(row)
                continue

            manual_spec = row.get(COL_MANUAL, "")
            row[COL_COMPARISON] = compare(manual_spec, payload)
            rows.append(row)

    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Done → {out}")


if __name__ == "__main__":
    process()