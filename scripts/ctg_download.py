"""
Fetch studies updated since yesterday from ClinicalTrials.gov API and
save the JSON response to a file in the `data/` folder.

Usage:
    python scripts/ctg_sync.py

The script no longer writes to the database.
"""

import os
import sys
import json
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

import httpx

CTG_API_BASE = "https://clinicaltrials.gov/api/v2/studies"
PAGE_SIZE = 100
BATCH_SIZE = 500


def get_last_import_date(cur) -> str:
    """Get the most recent imported_at date from ctg_import."""
    cur.execute("SELECT imported_at FROM ctg_import ORDER BY imported_at DESC LIMIT 1")
    row = cur.fetchone()
    if not row:
        print("Error: No record in ctg_import. Run admin_db_setup.py first.")
        sys.exit(1)
    return row[0].strftime("%Y-%m-%d")


def fetch_studies_since(since_date: str) -> list[dict]:
    """Fetch all studies updated since the given date from ClinicalTrials.gov API."""
    all_studies = []
    filter_expr = f"AREA[LastUpdatePostDate]RANGE[{since_date},MAX]"
    params = {
        "filter.overallStatus": "RECRUITING",
        "filter.advanced": filter_expr,
        "sort": "LastUpdatePostDate:desc",
        "pageSize": PAGE_SIZE,
    }

    with httpx.Client(timeout=60.0) as client:
        page = 1
        while True:
            response = client.get(CTG_API_BASE, params=params)
            response.raise_for_status()
            data = response.json()

            studies = data.get("studies", [])
            all_studies.extend(studies)
            print(
                f"  Page {page}: {len(studies)} studies (total so far: {len(all_studies)})"
            )

            next_token = data.get("nextPageToken")
            if not next_token or not studies:
                break

            params["pageToken"] = next_token
            page += 1

    return all_studies


def parse_study(study: dict) -> dict:
    """Parse a single study into row tuples for each ctg_* table."""
    ps = study.get("protocolSection", {})

    ident = ps.get("identificationModule", {})
    nct_id = ident.get("nctId")
    if not nct_id:
        return None

    org = ident.get("orgStudyIdInfo", {})
    org_info = ident.get("organization", {})

    st = ps.get("statusModule", {})
    start = st.get("startDateStruct", {})
    comp = st.get("completionDateStruct", {})
    last_update = st.get("lastUpdatePostDateStruct", {})

    desc = ps.get("descriptionModule", {})

    cond_mod = ps.get("conditionsModule", {})

    elig = ps.get("eligibilityModule", {})
    raw_criteria = elig.get("eligibilityCriteria") or ""
    inclusion = None
    exclusion = None
    raw_lower = raw_criteria.lower()
    exc_idx = raw_lower.find("exclusion criteria")
    if exc_idx != -1:
        inclusion = raw_criteria[:exc_idx].strip()
        exclusion = raw_criteria[exc_idx:].strip()
    else:
        inclusion = raw_criteria.strip() or None

    design = ps.get("designModule", {})
    di = design.get("designInfo", {})
    enroll = design.get("enrollmentInfo", {})

    return {
        "nct_id": nct_id,
        "identification": (
            nct_id,
            org.get("id"),
            org_info.get("fullName"),
            org_info.get("class"),
            ident.get("briefTitle"),
            ident.get("officialTitle"),
        ),
        "status": (
            nct_id,
            st.get("overallStatus"),
            start.get("date"),
            start.get("type"),
            comp.get("date"),
            comp.get("type"),
            last_update.get("date"),
        ),
        "description": (
            nct_id,
            desc.get("briefSummary"),
            desc.get("detailedDescription"),
        ),
        "conditions": [(nct_id, c) for c in cond_mod.get("conditions", [])],
        "keywords": [(nct_id, k) for k in cond_mod.get("keywords", [])],
        "eligibility": (
            nct_id,
            inclusion,
            exclusion,
            elig.get("healthyVolunteers"),
            elig.get("sex"),
            elig.get("minimumAge"),
            elig.get("maximumAge"),
            elig.get("stdAges") or [],
        ),
        "design": (
            nct_id,
            design.get("studyType"),
            design.get("phases") or [],
            di.get("allocation"),
            di.get("interventionModel"),
            di.get("primaryPurpose"),
            (di.get("maskingInfo") or {}).get("masking"),
            enroll.get("count"),
            enroll.get("type"),
        ),
    }


def batch_insert(cur, table, cols, rows, conflict="DO NOTHING"):
    if not rows:
        return
    placeholders = ",".join(["%s"] * len(cols))
    col_str = ",".join(cols)
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        args = ",".join(cur.mogrify(f"({placeholders})", row).decode() for row in batch)
        cur.execute(
            f"INSERT INTO {table} ({col_str}) VALUES {args} ON CONFLICT {conflict}"
        )


def upsert_studies(cur, studies: list[dict]):
    """Parse and upsert a list of studies into ctg_* tables."""
    id_rows = []
    status_rows = []
    desc_rows = []
    condition_rows = []
    keyword_rows = []
    elig_rows = []
    design_rows = []
    nct_ids = []

    for study in studies:
        parsed = parse_study(study)
        if not parsed:
            continue
        nct_ids.append(parsed["nct_id"])
        id_rows.append(parsed["identification"])
        status_rows.append(parsed["status"])
        desc_rows.append(parsed["description"])
        condition_rows.extend(parsed["conditions"])
        keyword_rows.extend(parsed["keywords"])
        elig_rows.append(parsed["eligibility"])
        design_rows.append(parsed["design"])

    # Delete existing conditions/keywords for updated studies so we get a clean replace
    if nct_ids:
        placeholders = ",".join(cur.mogrify("%s", (nid,)).decode() for nid in nct_ids)
        cur.execute(f"DELETE FROM ctg_condition WHERE nct_id IN ({placeholders})")
        cur.execute(f"DELETE FROM ctg_keyword WHERE nct_id IN ({placeholders})")

    batch_insert(
        cur,
        "ctg_identification",
        [
            "nct_id",
            "org_study_id",
            "org_name",
            "org_class",
            "brief_title",
            "official_title",
        ],
        id_rows,
        "(nct_id) DO UPDATE SET brief_title = EXCLUDED.brief_title, "
        "official_title = EXCLUDED.official_title, "
        "org_name = EXCLUDED.org_name, org_class = EXCLUDED.org_class",
    )

    batch_insert(
        cur,
        "ctg_status",
        [
            "nct_id",
            "overall_status",
            "start_date",
            "start_date_type",
            "completion_date",
            "completion_date_type",
            "last_update_date",
        ],
        status_rows,
        "(nct_id) DO UPDATE SET overall_status = EXCLUDED.overall_status, "
        "last_update_date = EXCLUDED.last_update_date",
    )

    batch_insert(
        cur,
        "ctg_description",
        ["nct_id", "brief_summary", "detailed_description"],
        desc_rows,
        "(nct_id) DO UPDATE SET brief_summary = EXCLUDED.brief_summary, "
        "detailed_description = EXCLUDED.detailed_description",
    )

    batch_insert(cur, "ctg_condition", ["nct_id", "condition"], condition_rows)

    batch_insert(cur, "ctg_keyword", ["nct_id", "keyword"], keyword_rows)

    batch_insert(
        cur,
        "ctg_eligibility",
        [
            "nct_id",
            "inclusion_criteria",
            "exclusion_criteria",
            "healthy_volunteers",
            "sex",
            "minimum_age",
            "maximum_age",
            "std_ages",
        ],
        elig_rows,
        "(nct_id) DO UPDATE SET inclusion_criteria = EXCLUDED.inclusion_criteria, "
        "exclusion_criteria = EXCLUDED.exclusion_criteria, "
        "healthy_volunteers = EXCLUDED.healthy_volunteers, "
        "sex = EXCLUDED.sex, minimum_age = EXCLUDED.minimum_age, "
        "maximum_age = EXCLUDED.maximum_age",
    )

    batch_insert(
        cur,
        "ctg_design",
        [
            "nct_id",
            "study_type",
            "phases",
            "allocation",
            "intervention_model",
            "primary_purpose",
            "masking",
            "enrollment_count",
            "enrollment_type",
        ],
        design_rows,
        "(nct_id) DO UPDATE SET study_type = EXCLUDED.study_type, "
        "phases = EXCLUDED.phases, enrollment_count = EXCLUDED.enrollment_count",
    )

    print(f"  Upserted {len(id_rows)} studies")


def main():
    # Use yesterday's UTC date as the since_date
    since_dt = datetime.now(timezone.utc) - timedelta(days=1)
    since_date = since_dt.strftime("%Y-%m-%d")
    print(f"Fetching studies updated since {since_date}...")

    studies = fetch_studies_since(since_date)
    print(f"Fetched {len(studies)} updated studies from ClinicalTrials.gov")

    # Save studies to a newline-delimited JSON file in data/
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    out_dir = os.path.normpath(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"ctg-studies-{since_date}.jsonl")
    with open(out_path, "w", encoding="utf-8") as fh:
        for study in studies:
            fh.write(json.dumps(study, ensure_ascii=False) + "\n")

    print(f"Wrote {len(studies)} studies to {out_path}")


if __name__ == "__main__":
    main()
