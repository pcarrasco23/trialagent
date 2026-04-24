"""
Index TREC 2021 corpus into Qdrant with BM25 sparse vectors.

Uses hashed token indices — no vocabulary file needed.
Qdrant's Modifier.IDF handles IDF weighting server-side.

Usage:
    python scripts/trec_corpus_bm25.py
    python scripts/trec_corpus_bm25.py --limit 1000

Requires: QDRANT_URL
"""

import argparse
import json
import os
from pathlib import Path

import tqdm
from nltk import word_tokenize
from qdrant_client import QdrantClient, models

from qdrant_utils import (
    nct_id_to_point_id, tokens_to_sparse_vector,
    ensure_collection, get_existing_nct_ids, filter_new_trials,
)

COLLECTION_NAME = "trec_2021_trial_corpus"
BATCH_SIZE = 64
CORPUS_PATH = Path(__file__).parent.parent / "clients" / "trec" / "data" / "corpus.jsonl"


def load_corpus(limit: int | None = None) -> list[dict]:
    trials = []
    with open(CORPUS_PATH) as f:
        for line in f:
            record = json.loads(line)
            meta = record.get("metadata", {})
            trials.append({
                "nct_id": record["_id"],
                "title": record.get("title", ""),
                "text": record.get("text", ""),
                "brief_title": meta.get("brief_title", ""),
                "phase": meta.get("phase", ""),
                "diseases": meta.get("diseases", ""),
                "drugs": meta.get("drugs", ""),
                "enrollment": meta.get("enrollment"),
                "inclusion_criteria": meta.get("inclusion_criteria", ""),
                "exclusion_criteria": meta.get("exclusion_criteria", ""),
                "brief_summary": meta.get("brief_summary", ""),
            })
            if limit and len(trials) >= limit:
                break
    return trials


def build_bm25_tokens(trial: dict) -> list[str]:
    title = trial.get("title") or trial.get("brief_title") or ""
    tokens = word_tokenize(title.lower()) * 3
    for disease in str(trial.get("diseases") or "").split(", "):
        disease = disease.strip("[]'\"")
        if disease:
            tokens += word_tokenize(disease.lower()) * 2
    text = trial.get("text") or trial.get("brief_summary") or ""
    tokens += word_tokenize(text.lower())
    return tokens


def build_payload(trial: dict) -> dict:
    return {
        "nct_id": trial["nct_id"],
        "brief_title": trial.get("brief_title") or "",
        "phase": trial.get("phase") or "",
        "diseases": trial.get("diseases") or "",
        "drugs": trial.get("drugs") or "",
        "enrollment": trial.get("enrollment"),
        "inclusion_criteria": trial.get("inclusion_criteria") or "",
        "exclusion_criteria": trial.get("exclusion_criteria") or "",
        "brief_summary": trial.get("brief_summary") or "",
    }


def main():
    parser = argparse.ArgumentParser(description="Index TREC corpus BM25 into Qdrant")
    parser.add_argument("--qdrant-url", default=os.environ.get("QDRANT_URL", ""))
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true", help="Re-index all trials, even if already indexed")
    args = parser.parse_args()

    if not args.qdrant_url:
        print("Error: QDRANT_URL is required")
        return

    print(f"Loading corpus from {CORPUS_PATH}...")
    trials = load_corpus(limit=args.limit)
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
