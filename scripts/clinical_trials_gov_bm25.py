"""
Index ctg_* trial data into Qdrant with BM25 sparse vectors.

Uses hashed token indices — no vocabulary file needed.
Qdrant's Modifier.IDF handles IDF weighting server-side.

Usage:
    python scripts/clinical_trials_gov_bm25.py
    python scripts/clinical_trials_gov_bm25.py --limit 1000

Requires: ADMIN_DB_URL, QDRANT_URL
"""

import argparse
import os

import json
import tqdm
from nltk import word_tokenize
from qdrant_client import QdrantClient, models

from qdrant_utils import (
    nct_id_to_point_id,
    tokens_to_sparse_vector,
    ensure_collection,
    get_existing_nct_ids,
    filter_new_trials,
)

COLLECTION_NAME = "clinical_trials_gov"
BATCH_SIZE = 64


def fetch_trials(json_path: str, limit: int | None = None) -> list[dict]:
    trials: list[dict] = []
    with open(json_path, "r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            obj = json.loads(line)
            ps = obj.get("protocolSection", {})
            idm = ps.get("identificationModule", {})
            desc = ps.get("descriptionModule", {})
            cond = ps.get("conditionsModule", {})
            elig = ps.get("eligibilityModule", {})
            design = ps.get("designModule", {})

            trial = {
                "nct_id": idm.get("nctId"),
                "brief_title": idm.get("briefTitle") or "",
                "official_title": idm.get("officialTitle") or "",
                "org_name": (idm.get("organization") or {}).get("fullName", "") if isinstance(idm.get("organization"), dict) else "",
                "org_class": (idm.get("organization") or {}).get("class", "") if isinstance(idm.get("organization"), dict) else "",
                "brief_summary": desc.get("briefSummary") or "",
                "detailed_description": desc.get("detailedDescription") or "",
                "inclusion_criteria": elig.get("eligibilityCriteria") or "",
                "exclusion_criteria": "",
                "healthy_volunteers": elig.get("healthyVolunteers"),
                "sex": elig.get("sex") or "",
                "minimum_age": elig.get("minimumAge") or "",
                "maximum_age": elig.get("maximumAge") or "",
                "study_type": design.get("studyType") or "",
                "phases": design.get("phases") or [],
                "enrollment_count": (design.get("enrollmentInfo") or {}).get("count"),
                "enrollment_type": (design.get("enrollmentInfo") or {}).get("type", ""),
                "conditions": ", ".join(cond.get("conditions") or []),
                "keywords": ", ".join(cond.get("keywords") or []),
            }
            trials.append(trial)
            if limit is not None and len(trials) >= limit:
                break
    return trials


def build_bm25_tokens(trial: dict) -> list[str]:
    title = trial.get("brief_title") or ""
    tokens = word_tokenize(title.lower()) * 3
    for condition in (trial.get("conditions") or "").split(", "):
        if condition:
            tokens += word_tokenize(condition.lower()) * 2
    for keyword in (trial.get("keywords") or "").split(", "):
        if keyword:
            tokens += word_tokenize(keyword.lower()) * 2
    summary = trial.get("brief_summary") or ""
    tokens += word_tokenize(summary.lower())
    return tokens


def build_payload(trial: dict) -> dict:
    return {
        "nct_id": trial["nct_id"],
        "brief_title": trial.get("brief_title") or "",
        "official_title": trial.get("official_title") or "",
        "org_name": trial.get("org_name") or "",
        "brief_summary": trial.get("brief_summary") or "",
        "detailed_description": trial.get("detailed_description") or "",
        "conditions": trial.get("conditions") or "",
        "keywords": trial.get("keywords") or "",
        "inclusion_criteria": trial.get("inclusion_criteria") or "",
        "exclusion_criteria": trial.get("exclusion_criteria") or "",
        "healthy_volunteers": trial.get("healthy_volunteers"),
        "sex": trial.get("sex") or "",
        "minimum_age": trial.get("minimum_age") or "",
        "maximum_age": trial.get("maximum_age") or "",
        "study_type": trial.get("study_type") or "",
        "phases": trial.get("phases") or [],
        "enrollment_count": trial.get("enrollment_count"),
        "enrollment_type": trial.get("enrollment_type") or "",
    }


def main():
    parser = argparse.ArgumentParser(
        description="Index clinical_trials_gov BM25 into Qdrant"
    )
    parser.add_argument("--qdrant-url", default=os.environ.get("QDRANT_URL", ""))
    parser.add_argument("--json-path", default=os.path.join("data", "ctg-studies.json"))
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-index all trials, even if already indexed",
    )
    args = parser.parse_args()

    if not args.qdrant_url:
        print("Error: QDRANT_URL is required")
        return

    json_path = args.json_path
    if not os.path.exists(json_path):
        print(f"Error: JSON file not found: {json_path}")
        return

    print(f"Fetching trials from JSON file: {json_path}...")
    trials = fetch_trials(json_path, limit=args.limit)
    print(f"  {len(trials)} trials loaded")
    if not trials:
        return

    client = QdrantClient(url=args.qdrant_url)
    ensure_collection(client, COLLECTION_NAME)

    if not args.force:
        existing = get_existing_nct_ids(client, COLLECTION_NAME)
        trials = filter_new_trials(trials, existing)
        if not trials:
            print("All trials already indexed.")
            return
    else:
        print("  --force: re-indexing all trials")

    print("Indexing BM25 vectors...")
    for i in tqdm.tqdm(range(0, len(trials), BATCH_SIZE)):
        batch = trials[i : i + BATCH_SIZE]

        points = [
            models.PointStruct(
                id=nct_id_to_point_id(batch[j]["nct_id"]),
                vector={
                    "bm25": tokens_to_sparse_vector(build_bm25_tokens(batch[j])),
                },
                payload=build_payload(batch[j]),
            )
            for j in range(len(batch))
        ]
        client.upsert(collection_name=COLLECTION_NAME, points=points)

    print(f"BM25 indexing complete. {len(trials)} trials.")


if __name__ == "__main__":
    main()
