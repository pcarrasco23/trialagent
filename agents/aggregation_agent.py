"""
Agent that subscribes to EligibilityResults events, fetches full trial details
from the ctg_studies file, and uses an LLM to produce relevance (R) and
eligibility (E) scores for each patient-trial pair.
Follows the same aggregation approach as TrialGPT/run_aggregation.py.
"""

import json
import os

from autogen_agentchat.base import Response
from autogen_agentchat.messages import TextMessage
from openai import OpenAI

from agents.event_bus import bus, AgentEvent
from agents.prompt_loader import get_prompt

openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
DEFAULT_MODEL = os.environ.get("AGGREGATION_MODEL", "gpt-4o")


def convert_criteria_pred_to_string(prediction: dict, trial_info: dict) -> str:
    """Convert criterion-level eligibility predictions to a readable string."""
    output = ""

    for inc_exc in ["inclusion", "exclusion"]:
        criteria_text = trial_info.get(f"{inc_exc}_criteria", "")
        if not criteria_text:
            continue

        idx2criterion = {}
        idx = 0
        for criterion in criteria_text.split("\n\n"):
            criterion = criterion.strip()
            if (
                "inclusion criteria" in criterion.lower()
                or "exclusion criteria" in criterion.lower()
            ):
                continue
            if len(criterion) < 5:
                continue
            idx2criterion[str(idx)] = criterion
            idx += 1

        pred_data = prediction.get(inc_exc, {})
        if not isinstance(pred_data, dict):
            continue

        for idx, (criterion_idx, preds) in enumerate(pred_data.items()):
            if criterion_idx not in idx2criterion:
                continue

            criterion = idx2criterion[criterion_idx]

            if not isinstance(preds, list) or len(preds) != 3:
                continue

            output += f"{inc_exc} criterion {idx}: {criterion}\n"
            output += f"\tPatient relevance: {preds[0]}\n"
            if len(preds[1]) > 0:
                output += f"\tEvident sentences: {preds[1]}\n"
            output += f"\tPatient eligibility: {preds[2]}\n"

    return output


def build_aggregation_prompt(
    patient: str, trial_results: dict, trial_info: dict
) -> tuple[str, str]:
    """Build system and user prompts for aggregation scoring."""
    trial = f"Title: {trial_info['brief_title']}\n"
    trial += f"Target conditions: {', '.join(trial_info['conditions_list'])}\n"
    trial += f"Summary: {trial_info.get('brief_summary', '')}"

    pred = convert_criteria_pred_to_string(trial_results, trial_info)

    system_prompt = get_prompt("aggregation_agent", "default", "system")
    user_template = get_prompt("aggregation_agent", "default", "user")
    user_prompt = user_template.format(patient=patient, trial=trial, predictions=pred)

    return system_prompt, user_prompt


def aggregate_trial(
    patient: str, trial_results: dict, trial_info: dict, model: str = DEFAULT_MODEL
) -> tuple[dict, int]:
    """Call LLM to produce relevance and eligibility scores for a patient-trial pair.
    Returns (scores, total_tokens)."""
    system_prompt, user_prompt = build_aggregation_prompt(
        patient, trial_results, trial_info
    )

    response = openai_client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )

    total_tokens = response.usage.total_tokens if response.usage else 0

    result = response.choices[0].message.content.strip()
    result = result.strip("`").strip("json")

    try:
        return json.loads(result), total_tokens
    except json.JSONDecodeError:
        return {"raw_output": result}, total_tokens


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

        model = bus.get_workflow_param("model", DEFAULT_MODEL)
        print(f"  [{self.name}] Aggregating scores using {model}...")
        all_scores = []
        self.last_total_tokens = 0

        for result in eligibility_results:
            nct_id = result["nct_id"]
            conditions = result.get("conditions", "")
            trial_info = {
                "nct_id": nct_id,
                "brief_title": result.get("brief_title", ""),
                "brief_summary": result.get("brief_summary", ""),
                "inclusion_criteria": result.get("inclusion_criteria", ""),
                "exclusion_criteria": result.get("exclusion_criteria", ""),
                "conditions_list": (
                    [c.strip() for c in conditions.split(",") if c.strip()]
                    if isinstance(conditions, str)
                    else conditions
                ),
            }

            eligibility = result.get("eligibility", {})
            if not isinstance(eligibility, dict):
                print(f"    Skipping {nct_id} — invalid eligibility data")
                continue

            print(f"    Scoring {nct_id}: {trial_info['brief_title'][:60]}...")

            scores, tokens = aggregate_trial(
                content, eligibility, trial_info, model=model
            )
            self.last_total_tokens += tokens

            all_scores.append(
                {
                    "nct_id": nct_id,
                    "brief_title": trial_info["brief_title"],
                    "relevance_score": scores.get("relevance_score_R", 0),
                    "eligibility_score": scores.get("eligibility_score_E", 0),
                    "relevance_explanation": scores.get("relevance_explanation", ""),
                    "eligibility_explanation": scores.get(
                        "eligibility_explanation", ""
                    ),
                    "matching": eligibility,
                }
            )

            print(
                f"      -> R={scores.get('relevance_score_R', '?')} "
                f"E={scores.get('eligibility_score_E', '?')}"
            )

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
