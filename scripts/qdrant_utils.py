"""
Shared utilities for Qdrant indexing scripts.
"""

import hashlib
from collections import Counter

import nltk
from nltk import word_tokenize
from qdrant_client import QdrantClient, models

nltk.download("punkt", quiet=True)
nltk.download("punkt_tab", quiet=True)

EMBEDDING_DIM = 768
# Use a large hash space to minimize collisions
SPARSE_VECTOR_HASH_SPACE = 2**31


def nct_id_to_point_id(nct_id: str) -> int:
    return int(hashlib.sha256(nct_id.encode()).hexdigest()[:15], 16)


def token_to_index(token: str) -> int:
    """Deterministic hash of a token to a sparse vector index. No vocab file needed."""
    return int(hashlib.md5(token.encode()).hexdigest(), 16) % SPARSE_VECTOR_HASH_SPACE


def tokens_to_sparse_vector(tokens: list[str]) -> models.SparseVector:
    """Convert a list of tokens to a Qdrant SparseVector using term frequency and hashed indices.
    Qdrant's Modifier.IDF handles IDF weighting server-side."""
    tf = Counter(tokens)
    indices = []
    values = []
    for token, count in tf.items():
        indices.append(token_to_index(token))
        values.append(float(count))
    return models.SparseVector(indices=indices, values=values)


def ensure_collection(client: QdrantClient, collection_name: str):
    existing = [c.name for c in client.get_collections().collections]
    if collection_name not in existing:
        client.create_collection(
            collection_name=collection_name,
            vectors_config={
                "medcpt": models.VectorParams(
                    size=EMBEDDING_DIM,
                    distance=models.Distance.COSINE,
                ),
            },
            sparse_vectors_config={
                "bm25": models.SparseVectorParams(
                    modifier=models.Modifier.IDF,
                ),
            },
        )
        print(f"  Created Qdrant collection '{collection_name}'")
    else:
        print(f"  Collection '{collection_name}' already exists")


def get_existing_nct_ids(client: QdrantClient, collection_name: str) -> set[str]:
    existing = set()
    offset = None
    while True:
        result = client.scroll(
            collection_name=collection_name,
            limit=1000,
            offset=offset,
            with_payload=["nct_id"],
            with_vectors=False,
        )
        points, next_offset = result
        for p in points:
            existing.add(p.payload.get("nct_id"))
        if next_offset is None:
            break
        offset = next_offset
    return existing


def filter_new_trials(trials: list[dict], existing_nct_ids: set[str]) -> list[dict]:
    new_trials = [t for t in trials if t["nct_id"] not in existing_nct_ids]
    print(f"  {len(existing_nct_ids)} trials already indexed")
    print(f"  {len(new_trials)} new trials to index")
    return new_trials
