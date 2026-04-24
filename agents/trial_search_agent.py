"""
Agent that subscribes to TrialKeywords events, runs hybrid retrieval
(MedCPT dense + BM25 sparse with RRF) against Qdrant, and prints matched trials.
"""

import hashlib
import json
import os
from collections import Counter
from pathlib import Path

import nltk
import torch
from autogen_agentchat.base import Response
from autogen_agentchat.messages import TextMessage
from nltk import word_tokenize
from qdrant_client import QdrantClient, models
from transformers import AutoModel, AutoTokenizer

from agents.event_bus import bus, AgentEvent

SPARSE_VECTOR_HASH_SPACE = 2**31

nltk.download("punkt", quiet=True)
nltk.download("punkt_tab", quiet=True)

COLLECTION_NAME = "clinical_trials_gov"
QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")

# RRF parameters (matching hybrid_fusion_retrieval.py)
RRF_K = 60
BM25_WEIGHT = 1
MEDCPT_WEIGHT = 1
TOP_N = 100  # results per retriever per condition


class TrialSearchAgent:
    """Listens for TrialKeywords, searches Qdrant with hybrid retrieval, prints ranked results."""

    def __init__(self, qdrant_url: str):
        self.name = "trial_search_agent"
        self.client = QdrantClient(url=qdrant_url)

        # Load MedCPT query encoder
        print(f"  [{self.name}] Loading MedCPT-Query-Encoder...")
        self.tokenizer = AutoTokenizer.from_pretrained("ncbi/MedCPT-Query-Encoder")
        self.model = AutoModel.from_pretrained("ncbi/MedCPT-Query-Encoder")
        self.model.eval()
        print(f"  [{self.name}] Ready")

    def _encode_query_dense(self, conditions: list[str]):
        """Encode conditions with MedCPT-Query-Encoder, return numpy array."""
        with torch.no_grad():
            encoded = self.tokenizer(
                conditions,
                truncation=True,
                padding=True,
                return_tensors="pt",
                max_length=256,
            )
            embeds = self.model(**encoded).last_hidden_state[:, 0, :].numpy()
        return embeds

    def _encode_query_sparse(self, condition: str) -> models.SparseVector:
        """Build a sparse query vector from condition text using hashed token indices."""
        tokens = word_tokenize(condition.lower())
        tf = Counter(tokens)
        indices = []
        values = []
        for token, count in tf.items():
            idx = (
                int(hashlib.md5(token.encode()).hexdigest(), 16)
                % SPARSE_VECTOR_HASH_SPACE
            )
            indices.append(idx)
            values.append(float(count))
        return models.SparseVector(indices=indices, values=values)

    def search(
        self,
        conditions: list[str],
        top_k: int = 50,
        collection_name: str = COLLECTION_NAME,
    ) -> list[dict]:
        """
        Hybrid search over Qdrant using both MedCPT and BM25 vectors,
        then combine with Reciprocal Rank Fusion (RRF) weighted by
        condition priority.
        """
        if not conditions:
            return []

        dense_embeds = self._encode_query_dense(conditions)
        nctid2score: dict[str, float] = {}

        for condition_idx, condition in enumerate(conditions):
            condition_weight = 1 / (condition_idx + 1)

            # MedCPT dense search
            if MEDCPT_WEIGHT > 0:
                medcpt_results = self.client.query_points(
                    collection_name=collection_name,
                    query=dense_embeds[condition_idx].tolist(),
                    using="medcpt",
                    limit=TOP_N,
                )
                for rank, point in enumerate(medcpt_results.points):
                    nct_id = point.payload.get("nct_id", "")
                    rrf_score = (1 / (rank + RRF_K)) * condition_weight * MEDCPT_WEIGHT
                    nctid2score[nct_id] = nctid2score.get(nct_id, 0) + rrf_score

            # BM25 sparse search
            if BM25_WEIGHT > 0:
                sparse_vec = self._encode_query_sparse(condition)
                if sparse_vec.indices:
                    bm25_results = self.client.query_points(
                        collection_name=collection_name,
                        query=sparse_vec,
                        using="bm25",
                        limit=TOP_N,
                    )
                    for rank, point in enumerate(bm25_results.points):
                        nct_id = point.payload.get("nct_id", "")
                        rrf_score = (
                            (1 / (rank + RRF_K)) * condition_weight * BM25_WEIGHT
                        )
                        nctid2score[nct_id] = nctid2score.get(nct_id, 0) + rrf_score

        # Sort by fused score
        ranked = sorted(nctid2score.items(), key=lambda x: -x[1])[:top_k]

        # Fetch payloads for top results
        results = []
        for nct_id, score in ranked:
            hits = self.client.scroll(
                collection_name=collection_name,
                scroll_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="nct_id",
                            match=models.MatchValue(value=nct_id),
                        )
                    ]
                ),
                limit=1,
            )[0]
            payload = hits[0].payload if hits else {}

            results.append(
                {
                    "nct_id": nct_id,
                    "score": round(score, 6),
                    "brief_title": payload.get("brief_title", ""),
                    "official_title": payload.get("official_title", ""),
                    "conditions": payload.get("conditions", ""),
                    "keywords": payload.get("keywords", ""),
                    "inclusion_criteria": payload.get("inclusion_criteria", ""),
                    "exclusion_criteria": payload.get("exclusion_criteria", ""),
                    "brief_summary": payload.get("brief_summary", ""),
                    "study_type": payload.get("study_type", ""),
                    "phases": payload.get("phases", []),
                    "enrollment_count": payload.get("enrollment_count"),
                }
            )

        return results

    async def on_messages(self, messages, cancellation_token=None):
        content = messages[0].content if messages else ""

        try:
            data = json.loads(content)
            conditions = data.get("conditions", [])
            patient_description = data.get("patient_description", "")
        except (json.JSONDecodeError, KeyError):
            print(f"  [{self.name}] Could not parse TrialKeywords payload")
            return Response(
                chat_message=TextMessage(
                    content="No conditions to search", source=self.name
                )
            )

        if not conditions:
            print(f"  [{self.name}] No conditions received, skipping search.")
            return Response(
                chat_message=TextMessage(
                    content="No conditions to search", source=self.name
                )
            )

        print(f"  [{self.name}] Searching with {len(conditions)} conditions...")

        top_k = bus.get_workflow_param("top_k", 20)
        trial_corpus = bus.get_workflow_param("trial_corpus", COLLECTION_NAME)
        print(f"  [{self.name}] Using collection: {trial_corpus}")
        results = self.search(conditions, top_k=top_k, collection_name=trial_corpus)

        print(f"\n  === Top {len(results)} Matched Clinical Trials ===")
        for i, trial in enumerate(results, 1):
            print(
                f"  {i:>3}. [{trial['nct_id']}] {trial['brief_title']}\n"
                f"       Conditions: {trial['conditions'][:80] or 'N/A'} | "
                f"Score: {trial['score']}"
            )

        # Broadcast top matches for downstream agents
        event = AgentEvent(
            message_type="TrialTopMatches",
            content=json.dumps(
                {
                    "patient_description": patient_description,
                    "trials": results,
                }
            ),
            workflow_id=bus.current_workflow_id(),
        )
        bus.schedule_broadcast(event)

        result_summary = f"Found {len(results)} matching trials."
        if results:
            top3 = ", ".join(r["nct_id"] for r in results[:3])
            result_summary += f" Top matches: {top3}"

        return Response(
            chat_message=TextMessage(content=result_summary, source=self.name)
        )


trial_search_agent = TrialSearchAgent(qdrant_url=QDRANT_URL)
bus.subscribe("TrialKeywords", trial_search_agent)
