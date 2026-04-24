"""
Index ctg_* trial data into Qdrant with MedCPT dense vectors.

Usage:
    python scripts/clinical_trials_gov_medcpt.py
    python scripts/clinical_trials_gov_medcpt.py --limit 1000

Requires: ADMIN_DB_URL, QDRANT_URL
"""

import argparse
import os

import json
import torch
import tqdm
from qdrant_client import QdrantClient, models
from transformers import AutoModel, AutoTokenizer

from qdrant_utils import (
    nct_id_to_point_id,
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

            trial = {
                "nct_id": idm.get("nctId"),
                "brief_title": idm.get("briefTitle") or "",
                "brief_summary": desc.get("briefSummary") or "",
                "inclusion_criteria": elig.get("eligibilityCriteria") or "",
                "exclusion_criteria": "",
                "conditions": ", ".join(cond.get("conditions") or []),
            }
            trials.append(trial)
            if limit is not None and len(trials) >= limit:
                break
    return trials


def build_embedding_text(trial: dict) -> tuple[str, str]:
    title = trial.get("brief_title") or ""
    parts = []
    if trial.get("conditions"):
        parts.append(f"Conditions: {trial['conditions']}")
    if trial.get("brief_summary"):
        parts.append(trial["brief_summary"])
    if trial.get("inclusion_criteria"):
        parts.append(f"Inclusion: {trial['inclusion_criteria']}")
    if trial.get("exclusion_criteria"):
        parts.append(f"Exclusion: {trial['exclusion_criteria']}")
    return title, " ".join(parts)


def main():
    parser = argparse.ArgumentParser(
        description="Index clinical_trials_gov MedCPT into Qdrant"
    )
    parser.add_argument("--qdrant-url", default=os.environ.get("QDRANT_URL", ""))
    parser.add_argument("--json-path", default=os.path.join("data", "ctg-studies.json"))
    parser.add_argument("--limit", type=int, default=None)
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

    existing = get_existing_nct_ids(client, COLLECTION_NAME)
    trials = filter_new_trials(trials, existing)
    if not trials:
        print("All trials already indexed.")
        return

    print("Loading MedCPT-Article-Encoder...")
    tokenizer = AutoTokenizer.from_pretrained("ncbi/MedCPT-Article-Encoder")
    model = AutoModel.from_pretrained("ncbi/MedCPT-Article-Encoder")
    model.eval()
    print("  Model loaded")

    print("Encoding and indexing MedCPT vectors...")
    for i in tqdm.tqdm(range(0, len(trials), BATCH_SIZE)):
        batch = trials[i : i + BATCH_SIZE]
        title_text_pairs = [build_embedding_text(t) for t in batch]

        with torch.no_grad():
            encoded = tokenizer(
                title_text_pairs,
                truncation=True,
                padding=True,
                return_tensors="pt",
                max_length=512,
            )
            embeddings = model(**encoded).last_hidden_state[:, 0, :].numpy()

        points = [
            models.PointStruct(
                id=nct_id_to_point_id(batch[j]["nct_id"]),
                vector={"medcpt": embeddings[j].tolist()},
                payload={"nct_id": batch[j]["nct_id"]},
            )
            for j in range(len(batch))
        ]
        client.upsert(collection_name=COLLECTION_NAME, points=points)

    print(f"MedCPT indexing complete. {len(trials)} trials.")


if __name__ == "__main__":
    main()
