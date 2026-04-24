"""
Agent that subscribes to AggregationResults events, computes a combined
ranking score from criterion-level matching and aggregation scores,
stores the final ranked list in the database, and broadcasts FinalRanking.
Follows the same ranking approach as old/rank_results.py.
"""

import json
import os

import psycopg2
from autogen_agentchat.base import Response
from autogen_agentchat.messages import TextMessage

from agents.event_bus import bus, AgentEvent

ADMIN_DB_URL = os.environ.get("ADMIN_DB_URL", "")
EPS = 1e-9


def get_matching_score(matching: dict) -> float:
    """Compute a matching score from criterion-level eligibility predictions."""
    included = 0
    not_inc = 0
    no_info_inc = 0

    excluded = 0

    for criteria, info in matching.get("inclusion", {}).items():
        if not isinstance(info, list) or len(info) != 3:
            continue
        label = info[2]
        if label == "included":
            included += 1
        elif label == "not included":
            not_inc += 1
        elif label == "not enough information":
            no_info_inc += 1

    for criteria, info in matching.get("exclusion", {}).items():
        if not isinstance(info, list) or len(info) != 3:
            continue
        if info[2] == "excluded":
            excluded += 1

    score = included / (included + not_inc + no_info_inc + EPS)

    if not_inc > 0:
        score -= 1

    if excluded > 0:
        score -= 1

    return score


def get_agg_score(result: dict) -> float:
    """Compute an aggregation score from relevance and eligibility scores."""
    try:
        rel_score = float(result.get("relevance_score", 0))
        eli_score = float(result.get("eligibility_score", 0))
    except (TypeError, ValueError):
        rel_score = 0
        eli_score = 0

    return (rel_score + eli_score) / 100


def save_ranking_results(workflow_id: str, ranked: list[dict]):
    """Persist the ranked trials to the ranking_results table."""
    if not ADMIN_DB_URL:
        return
    try:
        conn = psycopg2.connect(ADMIN_DB_URL)
        cur = conn.cursor()
        for i, trial in enumerate(ranked, 1):
            cur.execute(
                """INSERT INTO ranking_results
                   (workflow_id, nct_id, rank, combined_score, matching_score,
                    aggregation_score, relevance_score, eligibility_score, brief_title)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (workflow_id, nct_id) DO UPDATE SET
                       rank = EXCLUDED.rank,
                       combined_score = EXCLUDED.combined_score,
                       matching_score = EXCLUDED.matching_score,
                       aggregation_score = EXCLUDED.aggregation_score,
                       relevance_score = EXCLUDED.relevance_score,
                       eligibility_score = EXCLUDED.eligibility_score,
                       brief_title = EXCLUDED.brief_title""",
                (workflow_id, trial["nct_id"], i,
                 trial["combined_score"], trial["matching_score"],
                 trial["aggregation_score"], trial["relevance_score"],
                 trial["eligibility_score"], trial.get("brief_title", "")),
            )
        conn.commit()
        cur.close()
        conn.close()
        print(f"  [ranking_agent] Saved {len(ranked)} results to ranking_results")
    except Exception as e:
        print(f"  [ranking_agent] Failed to save results: {e}")


class RankingAgent:
    """Subscribes to AggregationResults, ranks trials by combined score."""

    def __init__(self):
        self.name = "ranking_agent"

    async def on_messages(self, messages, cancellation_token=None):
        content = messages[0].content if messages else ""

        try:
            agg_results = json.loads(content)
        except (json.JSONDecodeError, KeyError):
            print(f"  [{self.name}] Could not parse AggregationResults payload")
            return Response(
                chat_message=TextMessage(content="No results to rank", source=self.name)
            )

        if not agg_results:
            print(f"  [{self.name}] No results to rank.")
            return Response(
                chat_message=TextMessage(content="No results to rank", source=self.name)
            )

        print(f"  [{self.name}] Ranking {len(agg_results)} trials...")

        ranked = []
        for result in agg_results:
            matching = result.get("matching", {})
            matching_score = get_matching_score(matching)
            agg_score = get_agg_score(result)
            combined_score = matching_score + agg_score

            ranked.append({
                "nct_id": result["nct_id"],
                "brief_title": result.get("brief_title", ""),
                "matching_score": round(matching_score, 4),
                "aggregation_score": round(agg_score, 4),
                "combined_score": round(combined_score, 4),
                "relevance_score": result.get("relevance_score", 0),
                "eligibility_score": result.get("eligibility_score", 0),
            })

        ranked.sort(key=lambda x: x["combined_score"], reverse=True)

        print(f"\n  === Final Trial Ranking ({len(ranked)} trials) ===")
        for i, trial in enumerate(ranked, 1):
            print(
                f"  {i:>3}. [{trial['nct_id']}] {trial['brief_title'][:60]}\n"
                f"       Combined: {trial['combined_score']} "
                f"(matching: {trial['matching_score']}, "
                f"aggregation: {trial['aggregation_score']})"
            )

        # Save to database
        workflow_id = bus.current_workflow_id()
        save_ranking_results(workflow_id, ranked)

        result_text = json.dumps(ranked, indent=2)

        bus.schedule_broadcast(AgentEvent(
            message_type="FinalRanking",
            content=result_text,
            workflow_id=workflow_id,
        ))

        return Response(
            chat_message=TextMessage(content=result_text, source=self.name)
        )


ranking_agent = RankingAgent()
bus.subscribe("AggregationResults", ranking_agent)
