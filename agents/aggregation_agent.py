"""
Agent that subscribes to EligibilityResults events and computes relevance (R)
and eligibility (E) scores for each patient-trial pair using criterion-level
eligibility labels. No LLM call required.
"""

import json
import os

from autogen_agentchat.base import Response
from autogen_agentchat.messages import TextMessage
from agents.event_bus import bus, AgentEvent

INFORMATIVE_INCLUSION = {"included", "not included"}
INFORMATIVE_EXCLUSION = {"excluded", "not excluded"}
NEUTRAL = {"not enough information", "not applicable"}


def aggregate(criteria_predictions: list[dict]) -> float:
    """Compute eligibility score (E) from criterion-level predictions."""
    total = len(criteria_predictions)
    if total == 0:
        return 0.0

    informative = 0
    contribs = []

    for c in criteria_predictions:
        label, ctype = c["label"], c["type"]

        if label in NEUTRAL:
            contribs.append(0)
            continue

        if ctype == "inclusion":
            if label == "included":
                contribs.append(+1); informative += 1
            elif label == "not included":
                contribs.append(-1); informative += 1
        elif ctype == "exclusion":
            if label == "not excluded":
                contribs.append(+1); informative += 1
            elif label == "excluded":
                contribs.append(-1); informative += 1

    R = 100.0 * informative / total

    if any(c["type"] == "exclusion" and c["label"] == "excluded"
           for c in criteria_predictions):
        E = -R
    else:
        E = R * sum(contribs) / informative if informative else 0.0

    return max(-R, min(R, E))


def build_criteria_predictions(eligibility: dict) -> list[dict]:
    """Convert eligibility dict from eligibility agent into a flat list of predictions."""
    predictions = []
    for elig_type in ["inclusion", "exclusion"]:
        pred_data = eligibility.get(elig_type, {})
        if not isinstance(pred_data, dict):
            continue
        for criterion_idx, preds in pred_data.items():
            if not isinstance(preds, list) or len(preds) < 3:
                continue
            label = preds[2]
            predictions.append({"label": label, "type": elig_type})
    return predictions


def aggregate_trial(eligibility: dict) -> tuple[dict, int]:
    """Compute eligibility score from criterion-level predictions.
    Returns (scores, 0) — no tokens used."""
    predictions = build_criteria_predictions(eligibility)
    E = aggregate(predictions)
    return {
        "eligibility_score_E": round(E, 2),
    }, 0


class AggregationAgent:
    """Subscribes to EligibilityResults, produces relevance and eligibility scores."""

    def __init__(self):
        self.name = "aggregation_agent"
        self.last_total_tokens = 0

    async def on_messages(self, messages, cancellation_token=None):
        content = messages[0].content if messages else ""

        try:
            eligibility_results = json.loads(content)
        except (json.JSONDecodeError, KeyError):
            print(f"  [{self.name}] Could not parse EligibilityResults payload")
            return Response(
                chat_message=TextMessage(
                    content="No results to aggregate", source=self.name
                )
            )

        if not eligibility_results:
            print(f"  [{self.name}] No eligibility results, skipping.")
            return Response(
                chat_message=TextMessage(
                    content="No results to aggregate", source=self.name
                )
            )

        print(f"  [{self.name}] Aggregating scores...")
        all_scores = []
        self.last_total_tokens = 0

        for result in eligibility_results:
            nct_id = result["nct_id"]
            brief_title = result.get("brief_title", "")

            eligibility = result.get("eligibility", {})
            if not isinstance(eligibility, dict):
                print(f"    Skipping {nct_id} — invalid eligibility data")
                continue

            print(f"    Scoring {nct_id}: {brief_title[:60]}...")

            scores, tokens = aggregate_trial(eligibility)
            self.last_total_tokens += tokens

            all_scores.append(
                {
                    "nct_id": nct_id,
                    "brief_title": brief_title,
                    "relevance_score": result.get("retrieval_score", 0),
                    "eligibility_score": scores.get("eligibility_score_E", 0),
                    "matching": eligibility,
                }
            )

            print(f"      -> E={scores.get('eligibility_score_E', '?')}")

        # Sort by eligibility score descending
        all_scores.sort(key=lambda x: x["eligibility_score"], reverse=True)

        print(f"\n  === Aggregation Results ({len(all_scores)} trials) ===")
        for i, s in enumerate(all_scores, 1):
            print(
                f"  {i:>3}. [{s['nct_id']}] {s['brief_title'][:60]}\n"
                f"       Relevance: {s['relevance_score']} | "
                f"Eligibility: {s['eligibility_score']}"
            )

        result_text = json.dumps(all_scores, indent=2)

        bus.schedule_broadcast(
            AgentEvent(
                message_type="AggregationResults",
                content=result_text,
                workflow_id=bus.current_workflow_id(),
            )
        )

        return Response(chat_message=TextMessage(content=result_text, source=self.name))


aggregation_agent = AggregationAgent()
bus.subscribe("EligibilityResults", aggregation_agent)
