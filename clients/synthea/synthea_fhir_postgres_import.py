"""
Import FHIR Bundle JSON files into PostgreSQL.

Usage:
    python fhir_import.py <path_to_bundle.json> [--db-url postgresql://user:pass@host:port/dbname]

Requires: psycopg2-binary
    pip install psycopg2-binary
"""

import json
import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import Json

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
-- Raw storage: every resource kept verbatim
CREATE TABLE IF NOT EXISTS fhir_resource (
    id              TEXT PRIMARY KEY,
    resource_type   TEXT NOT NULL,
    full_url        TEXT,
    bundle_file     TEXT,
    resource        JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fhir_resource_type ON fhir_resource (resource_type);

-- Patient
CREATE TABLE IF NOT EXISTS patient (
    id              TEXT PRIMARY KEY,
    family_name     TEXT,
    given_names     TEXT,
    gender          TEXT,
    birth_date      DATE,
    address_line    TEXT,
    city            TEXT,
    state           TEXT,
    postal_code     TEXT,
    country         TEXT,
    phone           TEXT,
    marital_status  TEXT,
    race            TEXT,
    ethnicity       TEXT,
    birth_sex       TEXT
);

-- Encounter
CREATE TABLE IF NOT EXISTS encounter (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    status          TEXT,
    class_code      TEXT,
    type_code       TEXT,
    type_display    TEXT,
    period_start    TIMESTAMP,
    period_end      TIMESTAMP
);

-- Condition code lookup
CREATE TABLE IF NOT EXISTS condition_code (
    code            TEXT NOT NULL,
    code_system     TEXT NOT NULL,
    display         TEXT,
    PRIMARY KEY (code, code_system)
);

-- Condition
CREATE TABLE IF NOT EXISTS condition (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    encounter_id    TEXT,
    code            TEXT,
    clinical_status TEXT,
    verification    TEXT,
    category        TEXT,
    onset_datetime  TIMESTAMP,
    recorded_date   DATE
);

-- Observation code lookup
CREATE TABLE IF NOT EXISTS observation_code (
    code            TEXT NOT NULL,
    code_system     TEXT NOT NULL,
    display         TEXT,
    PRIMARY KEY (code, code_system)
);

-- Observation
CREATE TABLE IF NOT EXISTS observation (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    encounter_id    TEXT,
    status          TEXT,
    category_code   TEXT,
    code            TEXT,
    effective_dt    TIMESTAMP,
    value_quantity  NUMERIC,
    value_unit      TEXT,
    value_string    TEXT
);

-- Procedure
CREATE TABLE IF NOT EXISTS procedure (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    encounter_id    TEXT,
    status          TEXT,
    code            TEXT,
    code_display    TEXT,
    code_system     TEXT,
    performed_start TIMESTAMP,
    performed_end   TIMESTAMP
);

-- MedicationRequest
CREATE TABLE IF NOT EXISTS medication_request (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    encounter_id    TEXT,
    status          TEXT,
    intent          TEXT,
    medication_code TEXT,
    medication_display TEXT,
    authored_on     TIMESTAMP,
    reason_code     TEXT,
    reason_display  TEXT
);

-- Immunization
CREATE TABLE IF NOT EXISTS immunization (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    encounter_id    TEXT,
    status          TEXT,
    vaccine_code    TEXT,
    vaccine_display TEXT,
    occurrence_dt   TIMESTAMP,
    primary_source  BOOLEAN
);

-- DiagnosticReport
CREATE TABLE IF NOT EXISTS diagnostic_report (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    encounter_id    TEXT,
    status          TEXT,
    category_code   TEXT,
    code            TEXT,
    code_display    TEXT,
    effective_dt    TIMESTAMP,
    issued          TIMESTAMP
);

-- Claim
CREATE TABLE IF NOT EXISTS claim (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    status          TEXT,
    type_code       TEXT,
    use_code        TEXT,
    billable_start  TIMESTAMP,
    billable_end    TIMESTAMP,
    created         TIMESTAMP,
    total_value     NUMERIC,
    total_currency  TEXT
);

-- CarePlan
CREATE TABLE IF NOT EXISTS care_plan (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    encounter_id    TEXT,
    status          TEXT,
    intent          TEXT,
    category_code   TEXT,
    category_display TEXT,
    period_start    TIMESTAMP,
    period_end      TIMESTAMP
);

-- CareTeam
CREATE TABLE IF NOT EXISTS care_team (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    encounter_id    TEXT,
    status          TEXT,
    reason_code     TEXT,
    reason_display  TEXT,
    period_start    TIMESTAMP,
    period_end      TIMESTAMP
);

-- Device
CREATE TABLE IF NOT EXISTS device (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    status          TEXT,
    type_code       TEXT,
    type_display    TEXT,
    device_name     TEXT,
    udi_carrier     TEXT,
    manufacture_dt  DATE,
    expiration_dt   DATE,
    serial_number   TEXT
);

-- ExplanationOfBenefit
CREATE TABLE IF NOT EXISTS explanation_of_benefit (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    status          TEXT,
    type_code       TEXT,
    use_code        TEXT,
    outcome         TEXT,
    billable_start  TIMESTAMP,
    billable_end    TIMESTAMP,
    total_value     NUMERIC,
    total_currency  TEXT,
    payment_amount  NUMERIC
);

-- DocumentReference
CREATE TABLE IF NOT EXISTS document_reference (
    id              TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES patient(id),
    status          TEXT,
    type_code       TEXT,
    type_display    TEXT,
    category_code   TEXT,
    doc_date        TIMESTAMP,
    content_type    TEXT,
    content_url     TEXT
);

-- Patient trial workflow: one record per workflow run for a patient
CREATE TABLE IF NOT EXISTS patient_trial_workflow (
    id                  SERIAL PRIMARY KEY,
    patient_id          TEXT NOT NULL REFERENCES patient(id),
    trial_workflow_id   TEXT NOT NULL,
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ptw_patient ON patient_trial_workflow (patient_id);
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def ref_id(ref_str: str | None) -> str | None:
    """Extract the id from a FHIR reference like 'urn:uuid:abc-123' or 'Patient/abc-123'."""
    if not ref_str:
        return None
    if "/" in ref_str:
        return ref_str.rsplit("/", 1)[-1]
    if "urn:uuid:" in ref_str:
        return ref_str.replace("urn:uuid:", "")
    return ref_str


def first_coding(
    codeable_concept: dict | None,
) -> tuple[str | None, str | None, str | None]:
    """Return (code, display, system) from the first coding in a CodeableConcept."""
    if not codeable_concept:
        return None, None, None
    codings = codeable_concept.get("coding", [])
    if not codings:
        return None, codeable_concept.get("text"), None
    c = codings[0]
    return c.get("code"), c.get("display"), c.get("system")


def get_extension(extensions: list, url_suffix: str) -> dict | None:
    """Find an extension by URL suffix."""
    for ext in extensions or []:
        if ext.get("url", "").endswith(url_suffix):
            return ext
    return None


def ext_text(ext: dict | None) -> str | None:
    """Get the 'text' valueString from a complex extension."""
    if not ext:
        return None
    for sub in ext.get("extension", []):
        if sub.get("url") == "text":
            return sub.get("valueString")
    return ext.get("valueString") or ext.get("valueCode")


# ---------------------------------------------------------------------------
# Resource extractors — one per table
# ---------------------------------------------------------------------------


def extract_patient(r: dict) -> dict:
    exts = r.get("extension", [])
    name = (r.get("name") or [{}])[0]
    addr = (r.get("address") or [{}])[0]
    phone = None
    for t in r.get("telecom", []):
        if t.get("system") == "phone":
            phone = t.get("value")
            break
    return dict(
        id=r["id"],
        family_name=name.get("family"),
        given_names=" ".join(name.get("given", [])),
        gender=r.get("gender"),
        birth_date=r.get("birthDate"),
        address_line=", ".join(addr.get("line", [])),
        city=addr.get("city"),
        state=addr.get("state"),
        postal_code=addr.get("postalCode"),
        country=addr.get("country"),
        phone=phone,
        marital_status=r.get("maritalStatus", {}).get("text"),
        race=ext_text(get_extension(exts, "us-core-race")),
        ethnicity=ext_text(get_extension(exts, "us-core-ethnicity")),
        birth_sex=ext_text(get_extension(exts, "us-core-birthsex")),
    )


def extract_encounter(r: dict) -> dict:
    type_code, type_display, _ = first_coding((r.get("type") or [None])[0])
    period = r.get("period", {})
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("subject", {}).get("reference")),
        status=r.get("status"),
        class_code=r.get("class", {}).get("code"),
        type_code=type_code,
        type_display=type_display,
        period_start=period.get("start"),
        period_end=period.get("end"),
    )


def extract_condition(r: dict) -> dict:
    code, display, system = first_coding(r.get("code"))
    cat_code, _, _ = first_coding((r.get("category") or [None])[0])
    cs_code, _, _ = first_coding(r.get("clinicalStatus"))
    vs_code, _, _ = first_coding(r.get("verificationStatus"))
    # Collect lookup entry for condition_code table
    if code and system:
        _condition_codes[(code, system)] = display
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("subject", {}).get("reference")),
        encounter_id=ref_id(r.get("encounter", {}).get("reference")),
        code=code,
        clinical_status=cs_code,
        verification=vs_code,
        category=cat_code,
        onset_datetime=r.get("onsetDateTime"),
        recorded_date=r.get("recordedDate"),
    )


# Accumulated condition code lookups (populated during extraction)
_condition_codes: dict[tuple[str, str], str | None] = {}


def extract_observation(r: dict) -> dict:
    code, display, system = first_coding(r.get("code"))
    cat_code, _, _ = first_coding((r.get("category") or [None])[0])
    vq = r.get("valueQuantity", {})
    if code and system:
        _observation_codes[(code, system)] = display
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("subject", {}).get("reference")),
        encounter_id=ref_id(r.get("encounter", {}).get("reference")),
        status=r.get("status"),
        category_code=cat_code,
        code=code,
        effective_dt=r.get("effectiveDateTime"),
        value_quantity=vq.get("value"),
        value_unit=vq.get("unit"),
        value_string=r.get("valueString")
        or r.get("valueCodeableConcept", {}).get("text"),
    )


_observation_codes: dict[tuple[str, str], str | None] = {}


def extract_procedure(r: dict) -> dict:
    code, display, system = first_coding(r.get("code"))
    pp = r.get("performedPeriod", {})
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("subject", {}).get("reference")),
        encounter_id=ref_id(r.get("encounter", {}).get("reference")),
        status=r.get("status"),
        code=code,
        code_display=display,
        code_system=system,
        performed_start=pp.get("start"),
        performed_end=pp.get("end"),
    )


def extract_medication_request(r: dict) -> dict:
    med_ref = r.get("medicationReference", {}).get("reference")
    med_code, med_display, _ = first_coding(r.get("medicationCodeableConcept"))
    reason = (r.get("reasonCode") or [None])[0]
    rc, rd, _ = first_coding(reason)
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("subject", {}).get("reference")),
        encounter_id=ref_id(r.get("encounter", {}).get("reference")),
        status=r.get("status"),
        intent=r.get("intent"),
        medication_code=med_code or ref_id(med_ref),
        medication_display=med_display
        or (r.get("medicationReference", {}).get("display")),
        authored_on=r.get("authoredOn"),
        reason_code=rc,
        reason_display=rd,
    )


def extract_immunization(r: dict) -> dict:
    vc, vd, _ = first_coding(r.get("vaccineCode"))
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("patient", {}).get("reference")),
        encounter_id=ref_id(r.get("encounter", {}).get("reference")),
        status=r.get("status"),
        vaccine_code=vc,
        vaccine_display=vd,
        occurrence_dt=r.get("occurrenceDateTime"),
        primary_source=r.get("primarySource"),
    )


def extract_diagnostic_report(r: dict) -> dict:
    code, display, _ = first_coding(r.get("code"))
    cat_code, _, _ = first_coding((r.get("category") or [None])[0])
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("subject", {}).get("reference")),
        encounter_id=ref_id(r.get("encounter", {}).get("reference")),
        status=r.get("status"),
        category_code=cat_code,
        code=code,
        code_display=display,
        effective_dt=r.get("effectiveDateTime"),
        issued=r.get("issued"),
    )


def extract_claim(r: dict) -> dict:
    tc, _, _ = first_coding(r.get("type"))
    bp = r.get("billablePeriod", {})
    total = r.get("total", {})
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("patient", {}).get("reference")),
        status=r.get("status"),
        type_code=tc,
        use_code=r.get("use"),
        billable_start=bp.get("start"),
        billable_end=bp.get("end"),
        created=r.get("created"),
        total_value=total.get("value"),
        total_currency=total.get("currency"),
    )


def extract_care_plan(r: dict) -> dict:
    cc, cd, _ = first_coding((r.get("category") or [None])[0])
    period = r.get("period", {})
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("subject", {}).get("reference")),
        encounter_id=ref_id(r.get("encounter", {}).get("reference")),
        status=r.get("status"),
        intent=r.get("intent"),
        category_code=cc,
        category_display=cd,
        period_start=period.get("start"),
        period_end=period.get("end"),
    )


def extract_care_team(r: dict) -> dict:
    rc, rd, _ = first_coding((r.get("reasonCode") or [None])[0])
    period = r.get("period", {})
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("subject", {}).get("reference")),
        encounter_id=ref_id(r.get("encounter", {}).get("reference")),
        status=r.get("status"),
        reason_code=rc,
        reason_display=rd,
        period_start=period.get("start"),
        period_end=period.get("end"),
    )


def extract_device(r: dict) -> dict:
    tc, td, _ = first_coding(r.get("type"))
    dn = (r.get("deviceName") or [{}])[0]
    udi = (r.get("udiCarrier") or [{}])[0]
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("patient", {}).get("reference")),
        status=r.get("status"),
        type_code=tc,
        type_display=td,
        device_name=dn.get("name"),
        udi_carrier=udi.get("carrierHRF"),
        manufacture_dt=r.get("manufactureDate"),
        expiration_dt=r.get("expirationDate"),
        serial_number=r.get("serialNumber"),
    )


def extract_eob(r: dict) -> dict:
    tc, _, _ = first_coding(r.get("type"))
    bp = r.get("billablePeriod", {})
    totals = r.get("total", [])
    total_val = total_cur = None
    for t in totals:
        if t.get("category", {}).get("coding", [{}])[0].get("code") == "submitted":
            total_val = t.get("amount", {}).get("value")
            total_cur = t.get("amount", {}).get("currency")
            break
    if total_val is None and totals:
        total_val = totals[0].get("amount", {}).get("value")
        total_cur = totals[0].get("amount", {}).get("currency")
    payment = r.get("payment", {}).get("amount", {}).get("value")
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("patient", {}).get("reference")),
        status=r.get("status"),
        type_code=tc,
        use_code=r.get("use"),
        outcome=r.get("outcome"),
        billable_start=bp.get("start"),
        billable_end=bp.get("end"),
        total_value=total_val,
        total_currency=total_cur,
        payment_amount=payment,
    )


def extract_document_reference(r: dict) -> dict:
    tc, td, _ = first_coding(r.get("type"))
    cc, _, _ = first_coding((r.get("category") or [None])[0])
    content = (r.get("content") or [{}])[0].get("attachment", {})
    return dict(
        id=r["id"],
        patient_id=ref_id(r.get("subject", {}).get("reference")),
        status=r.get("status"),
        type_code=tc,
        type_display=td,
        category_code=cc,
        doc_date=r.get("date"),
        content_type=content.get("contentType"),
        content_url=content.get("url"),
    )


# Map resource type -> (table name, extractor function)
EXTRACTORS = {
    "Patient": ("patient", extract_patient),
    "Encounter": ("encounter", extract_encounter),
    "Condition": ("condition", extract_condition),
    "Observation": ("observation", extract_observation),
    "Procedure": ("procedure", extract_procedure),
    "MedicationRequest": ("medication_request", extract_medication_request),
    "Immunization": ("immunization", extract_immunization),
    "DiagnosticReport": ("diagnostic_report", extract_diagnostic_report),
    "Claim": ("claim", extract_claim),
    "CarePlan": ("care_plan", extract_care_plan),
    "CareTeam": ("care_team", extract_care_team),
    "Device": ("device", extract_device),
    "ExplanationOfBenefit": ("explanation_of_benefit", extract_eob),
    "DocumentReference": ("document_reference", extract_document_reference),
}

# Insertion order matters for foreign keys
INSERTION_ORDER = [
    "Patient",
    "Encounter",
    "Condition",
    "Observation",
    "Procedure",
    "MedicationRequest",
    "Immunization",
    "DiagnosticReport",
    "Claim",
    "CarePlan",
    "CareTeam",
    "Device",
    "ExplanationOfBenefit",
    "DocumentReference",
]

# ---------------------------------------------------------------------------
# Upsert helper
# ---------------------------------------------------------------------------


def upsert_rows(cur, table: str, rows: list[dict]):
    """Insert rows with ON CONFLICT DO UPDATE for idempotent loads."""
    if not rows:
        return
    cols = list(rows[0].keys())
    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "id")

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {update_set}
    """
    values = [tuple(row[c] for c in cols) for row in rows]
    cur.executemany(sql, values)


# ---------------------------------------------------------------------------
# Main import logic
# ---------------------------------------------------------------------------


def import_bundle(bundle_path: str, db_url: str):
    path = Path(bundle_path)
    with open(path) as f:
        bundle = json.load(f)

    if bundle.get("resourceType") != "Bundle":
        print(
            f"Error: {path.name} is not a FHIR Bundle (resourceType={bundle.get('resourceType')})"
        )
        sys.exit(1)

    entries = bundle.get("entry", [])
    print(f"Loaded bundle: {path.name} ({len(entries)} entries)")

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    # Create schema
    cur.execute(SCHEMA_SQL)
    conn.commit()

    # 1) Insert all resources into the raw fhir_resource table
    raw_rows = []
    for entry in entries:
        resource = entry.get("resource", {})
        raw_rows.append(
            (
                resource.get("id"),
                resource.get("resourceType"),
                entry.get("fullUrl"),
                path.name,
                Json(resource),
            )
        )

    cur.executemany(
        """INSERT INTO fhir_resource (id, resource_type, full_url, bundle_file, resource)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (id) DO UPDATE SET resource = EXCLUDED.resource,
           full_url = EXCLUDED.full_url, bundle_file = EXCLUDED.bundle_file""",
        raw_rows,
    )
    print(f"  fhir_resource: {len(raw_rows)} rows")

    # 2) Extract into typed tables in FK-safe order
    by_type: dict[str, list[dict]] = {}
    for entry in entries:
        resource = entry.get("resource", {})
        rt = resource.get("resourceType")
        if rt in EXTRACTORS:
            table, extractor = EXTRACTORS[rt]
            row = extractor(resource)
            by_type.setdefault(rt, []).append((table, row))

    # Insert observation code lookup entries
    if _observation_codes:
        cur.executemany(
            """INSERT INTO observation_code (code, code_system, display)
               VALUES (%s, %s, %s)
               ON CONFLICT (code, code_system) DO UPDATE SET display = EXCLUDED.display""",
            [
                (code, system, display)
                for (code, system), display in _observation_codes.items()
            ],
        )
        print(f"  observation_code: {len(_observation_codes)} rows")
        _observation_codes.clear()

    # Insert condition code lookup entries
    if _condition_codes:
        cur.executemany(
            """INSERT INTO condition_code (code, code_system, display)
               VALUES (%s, %s, %s)
               ON CONFLICT (code, code_system) DO UPDATE SET display = EXCLUDED.display""",
            [
                (code, system, display)
                for (code, system), display in _condition_codes.items()
            ],
        )
        print(f"  condition_code: {len(_condition_codes)} rows")
        _condition_codes.clear()

    for rt in INSERTION_ORDER:
        if rt not in by_type:
            continue
        table = by_type[rt][0][0]
        rows = [row for _, row in by_type[rt]]
        upsert_rows(cur, table, rows)
        print(f"  {table}: {len(rows)} rows")

    conn.commit()
    cur.close()
    conn.close()
    print("Import complete.")


FHIR_DATA_PATH = (
    Path(__file__).parent.parent
    / "synthea"
    / "data"
    / "synthea_sample_data_fhir_latest"
)


def ensure_schema(db_url: str):
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    cur.close()
    conn.close()
    print("Schema created/verified.")


def main():
    schema_only = "--schema-only" in sys.argv

    db_url = os.environ.get("SYNTHEA_FHIR_DB_URL", "")
    if not db_url:
        print("Error: SYNTHEA_FHIR_DB_URL environment variable is required")
        return

    if schema_only:
        ensure_schema(db_url)
        return

    if not FHIR_DATA_PATH.exists():
        print(f"Error: {FHIR_DATA_PATH} not found")
        return

    if FHIR_DATA_PATH.is_dir():
        files = sorted(FHIR_DATA_PATH.glob("*.json"))
        print(f"Found {len(files)} JSON files in {FHIR_DATA_PATH}")
        for f in files:
            import_bundle(str(f), db_url)
    else:
        import_bundle(str(FHIR_DATA_PATH), db_url)


if __name__ == "__main__":
    main()
