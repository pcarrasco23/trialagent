"""
Agent that subscribes to FinalRanking events and evaluates the ranking
against qrels (if available on the workflow) using pytrec_eval metrics.
"""

import json
import os

import psycopg2
import pytrec_eval
from autogen_agentchat.base import Response
from autogen_agentchat.messages import TextMessage

from agents.event_bus import bus
from lib.workflow import get_workflow_qrels

ADMIN_DB_URL = os.environ.get("ADMIN_DB_URL", "")

EVAL_METRICS = {"ndcg_cut_5", "ndcg_cut_10", "ndcg_cut_20", "P_10", "recip_rank"}


class QrelsAgent:
    """Subscribes to FinalRanking, evaluates against qrels if present."""

    def __init__(self):
        self.name = "qrels_agent"

    async def on_messages(self, messages, cancellation_token=None):
        content = messages[0].content if messages else ""

        workflow_id = bus.current_workflow_id()
        qrels = get_workflow_qrels(workflow_id)

        if not qrels:
            print(f"  [{self.name}] No qrels for this workflow, skipping.")
            return Response(
                chat_message=TextMessage(content="No qrels available", source=self.name)
            )

        try:
            ranked = json.loads(content)
        except (json.JSONDecodeError, KeyError):
            print(f"  [{self.name}] Could not parse FinalRanking payload")
            return Response(
                chat_message=TextMessage(content="No results to evaluate", source=self.name)
            )

        if not ranked:
            print(f"  [{self.name}] No ranking results to evaluate.")
            return Response(
                chat_message=TextMessage(content="No results to evaluate", source=self.name)
            )

        # Build pytrec_eval structures
        # qrels from DB: {nct_id: relevance} — need to wrap in {topic_id: {nct_id: relevance}}
        topic_id = "0"
        topic_qrels = {topic_id: {k: int(v) for k, v in qrels.items()}}

        my_results = {
            topic_id: {
                r["nct_id"]: float(r.get("combined_score") or 0)
                for r in ranked
            }
        }

        print(f"  [{self.name}] Evaluating {len(ranked)} ranked trials against {len(qrels)} qrels...")

        evaluator = pytrec_eval.RelevanceEvaluator(topic_qrels, EVAL_METRICS)
        eval_results = evaluator.evaluate(my_results)
        metrics = eval_results.get(topic_id, {})

        print(f"  [{self.name}] Evaluation results:")
        for metric, score in sorted(metrics.items()):
            print(f"    {metric}: {score:.4f}")

        # Persist metrics to database
        if ADMIN_DB_URL and metrics:
            try:
                conn = psycopg2.connect(ADMIN_DB_URL)
                cur = conn.cursor()
                for metric_name, metric_value in metrics.items():
                    cur.execute(
                        """INSERT INTO workflow_qrels_results (workflow_id, metric_name, metric_value)
                           VALUES (%s, %s, %s)
                           ON CONFLICT (workflow_id, metric_name) DO UPDATE SET metric_value = EXCLUDED.metric_value""",
                        (workflow_id, metric_name, float(metric_value)),
                    )
                conn.commit()
                cur.close()
                conn.close()
                print(f"  [{self.name}] Saved {len(metrics)} metrics to database.")
            except Exception as e:
                print(f"  [{self.name}] Failed to save metrics: {e}")

        result_text = json.dumps(metrics)

        return Response(
            chat_message=TextMessage(content=result_text, source=self.name)
        )


qrels_agent = QrelsAgent()
bus.subscribe("FinalRanking", qrels_agent)
