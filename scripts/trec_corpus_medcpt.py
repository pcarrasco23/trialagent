"""
Index TREC 2021 corpus into Qdrant with MedCPT dense vectors.

Usage:
    python scripts/trec_corpus_medcpt.py
    python scripts/trec_corpus_medcpt.py --limit 1000

Requires: QDRANT_URL
"""

import argparse
import json
import os
from pathlib import Path

import torch
import tqdm
from qdrant_client import QdrantClient, models
from transformers import AutoModel, AutoTokenizer

from qdrant_utils import (
    nct_id_to_point_id, ensure_collection, get_existing_nct_ids,
    filter_new_trials,
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
                "diseases": meta.get("diseases", ""),
                "inclusion_criteria": meta.get("inclusion_criteria", ""),
                "exclusion_criteria": meta.get("exclusion_criteria", ""),
                "brief_summary": meta.get("brief_summary", ""),
            })
            if limit and len(trials) >= limit:
                break
    return trials


def build_embedding_text(trial: dict) -> tuple[str, str]:
    title = trial.get("title") or trial.get("brief_title") or ""
    parts = []
    if trial.get("diseases"):
        parts.append(f"Conditions: {trial['diseases']}")
    if trial.get("text"):
        parts.append(trial["text"])
    elif trial.get("brief_summary"):
        parts.append(trial["brief_summary"])
    if trial.get("inclusion_criteria"):
        parts.append(f"Inclusion: {trial['inclusion_criteria']}")
    if trial.get("exclusion_criteria"):
        parts.append(f"Exclusion: {trial['exclusion_criteria']}")
    return title, " ".join(parts)


def main():
    parser = argparse.ArgumentParser(description="Index TREC corpus MedCPT into Qdrant")
    parser.add_argument("--qdrant-url", default=os.environ.get("QDRANT_URL", ""))
    parser.add_argument("--limit", type=int, default=None)
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
