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

# Attributes to ignore in Missing section (non-vital)
IGNORE_ATTRIBUTES = {
    "category", "image 1", "image 2", "image 3", "image 4"
}


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

def extract_manual_values(manual_text: str) -> set:
    """
    Extract all normalized values from manual text using common patterns.
    Returns a set of normalized values for comparison.
    This is more scalable than hardcoded attribute detection.
    """
    manual = normalize_value(manual_text)
    
    values = set()
    
    # Extract numeric-unit patterns (wattage, lumens, temperature, etc.)
    # Matches: 10w, 9.4w, 800lm, 2700k, 15000 hours, 2 year, etc.
    numeric_patterns = [
        r'\d+(?:\.\d+)?w\b',           # Wattage: 10w, 9.4w
        r'\d+lm\b',                     # Lumens: 800lm
        r'\d{3,4}k\b',                  # Color temp: 2700k, 4000k
        r'\d+(?:,\d{3})*\s*hours?\b',  # Hours: 15000 hours, 15,000 hours
        r'\d+\s*years?\b',              # Years: 2 year, 3 years
        r'\d+(?:\.\d+)?\s*v\b',         # Voltage: 1.2v, 220v
        r'\d+(?:\.\d+)?\s*mm\b',        # Dimensions: 119mm
        r'\d+(?:\.\d+)?\s*g\b',         # Weight: 526g
        r'\d+(?:\.\d+)?\s*mhz\b',       # Frequency: 2412mhz
    ]
    
    for pattern in numeric_patterns:
        matches = re.findall(pattern, manual)
        values.update(matches)
    
    # Extract common cap fittings
    cap_fittings = re.findall(r'\b(b22|e27|e14|gu10|g9|integrated)\b', manual)
    values.update(cap_fittings)
    
    # Extract color descriptions
    colors = re.findall(r'\b(warm white|cool white|daylight|rgb|blue|red|green)\b', manual)
    values.update(colors)
    
    # Extract dimmable
    if re.search(r'\bdimmable\b', manual):
        values.add('dimmable')
    
    return values


# ================= MAIN COMPARISON ENGINE =================

def compare(manual_text: str, payload_json: dict) -> str:
    """Full specification comparison using workflow values → manual text."""
    manual = normalize(manual_text)
    spec = extract_specification(payload_json)

    matched = []
    mismatched = []
    missing = []
    
    # Track all matched values (normalized) to prevent duplicate reporting
    matched_values = set()

    # ---- 1. Workflow → Manual comparison ----
    for key, val in spec.items():
        key_norm = key.lower().strip()
        val_norm = normalize_value(val)

        # Skip non-vital attributes for missing detection
        if key_norm in IGNORE_ATTRIBUTES:
            continue

        if not val_norm:
            continue

        is_match, mismatch_value = compare_attribute(val_norm, manual)

        if is_match:
            matched.append(f"{key}: {val}")
            matched_values.add(val_norm)  # Track matched value
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

    # ---- 2. Manual → Workflow (detect values in manual not found in workflow) ----
    # Extract all values from manual text
    manual_values = extract_manual_values(manual_text)
    
    # Check which manual values are NOT in the matched workflow values
    unmatched_manual_values = manual_values - matched_values
    
    # Only report genuinely missing values (not just different attribute names)
    if unmatched_manual_values:
        # Group similar unmatched values for cleaner reporting
        for manual_val in sorted(unmatched_manual_values):
            # Try to fuzzy match against all workflow values to avoid false positives
            is_similar = False
            for workflow_val in matched_values:
                if fuzz.partial_ratio(manual_val, workflow_val) >= 85:
                    is_similar = True
                    break
            
            if not is_similar:
                missing.append(
                    f"Unmatched Manual Value\n"
                    f"  Manual: {manual_val}\n"
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