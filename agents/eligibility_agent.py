"""
Agent that subscribes to TrialTopMatches events, uses an LLM to evaluate
patient eligibility against each trial's inclusion and exclusion criteria.
Follows the same matching approach as TrialGPT/run_matching.py.
"""

import json
import os

from autogen_agentchat.base import Response
from autogen_agentchat.messages import TextMessage
from nltk.tokenize import sent_tokenize
import psycopg2

from agents.event_bus import bus, AgentEvent
from agents.prompt_loader import get_prompt
from lib.llm_client import get_llm_client
from lib.workflow import get_workflow_observations

DEFAULT_MODEL = os.environ.get("ELIGIBILITY_MODEL", "gpt-4o")
ADMIN_DB_URL = os.environ.get("ADMIN_DB_URL", "")


def parse_criteria(criteria: str) -> str:
    """Number and clean criteria text."""
    output = ""
    idx = 0
    for criterion in criteria.split("\n\n"):
        criterion = criterion.strip()
        if (
            "inclusion criteria" in criterion.lower()
            or "exclusion criteria" in criterion.lower()
        ):
            continue
        if len(criterion) < 5:
            continue
        output += f"{idx}. {criterion}\n"
        idx += 1
    return output


def format_trial(trial: dict, inc_exc: str) -> str:
    """Format trial info for the LLM prompt."""
    diseases = trial.get("diseases", "")
    drugs = trial.get("drugs", "")

    text = f"Title: {trial['brief_title']}\n"
    text += f"Target diseases: {diseases}\n"
    text += f"Interventions: {drugs}\n"
    text += f"Summary: {trial.get('brief_summary', '')}\n"

    if inc_exc == "inclusion" and trial.get("inclusion_criteria"):
        text += f"Inclusion criteria:\n {parse_criteria(trial['inclusion_criteria'])}\n"
    elif inc_exc == "exclusion" and trial.get("exclusion_criteria"):
        text += f"Exclusion criteria:\n {parse_criteria(trial['exclusion_criteria'])}\n"

    return text


def build_matching_prompt(trial: dict, inc_exc: str, patient: str) -> tuple[str, str]:
    """Build system and user prompts for eligibility matching."""
    system_prompt = get_prompt("eligibility_agent", inc_exc, "system")
    user_template = get_prompt("eligibility_agent", "default", "user")
    user_prompt = user_template.format(
        patient=patient, trial=format_trial(trial, inc_exc)
    )
    return system_prompt, user_prompt


def evaluate_trial(
    trial: dict,
    patient_note: str,
    model: str = DEFAULT_MODEL,
    client=None,
    resolved_model: str = None,
) -> tuple[dict, int, list]:
    """Evaluate a single trial's inclusion and exclusion criteria against the patient.
    Returns (results, total_tokens, llm_chats)."""
    if client is None:
        client, resolved_model = get_llm_client(model)
    results = {}
    total_tokens = 0
    llm_chats = []

    for inc_exc in ["inclusion", "exclusion"]:
        criteria_text = trial.get(f"{inc_exc}_criteria", "")
        if not criteria_text or len(criteria_text.strip()) < 5:
            results[inc_exc] = {}
            continue

        system_prompt, user_prompt = build_matching_prompt(trial, inc_exc, patient_note)

        response = client.chat.completions.create(
            model=resolved_model or model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
        )

        if response.usage:
            total_tokens += response.usage.total_tokens

        raw_output = response.choices[0].message.content.strip()

        # Strip <think>...</think> tags (e.g. from Qwen models)
        import re
        raw_output = re.sub(r"<think>.*?</think>", "", raw_output, flags=re.DOTALL).strip()

        llm_chats.append(
            {
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                    {"role": "assistant", "content": raw_output},
                ]
            }
        )

        message = raw_output.strip("`").strip("json")

        try:
            results[inc_exc] = json.loads(message)
        except json.JSONDecodeError:
            results[inc_exc] = message

    return results, total_tokens, llm_chats


def prepare_patient_note(patient_description: str) -> str:
    """Number sentences in the patient description for criterion-level matching."""
    sents = sent_tokenize(patient_description)
    sents.append(
        "The patient will provide informed consent, and will comply with the "
        "trial protocol without any practical issues."
    )
    return "\n".join(f"{idx}. {sent}" for idx, sent in enumerate(sents))


def summarize_eligibility(results: dict) -> str:
    """Summarize the eligibility results into a readable verdict."""
    included = 0
    not_included = 0
    excluded = 0
    not_excluded = 0
    not_enough = 0

    for criteria in results.values():
        if not isinstance(criteria, dict):
            continue
        for values in criteria.values():
            if not isinstance(values, list) or len(values) < 3:
                continue
            label = values[2].lower()
            if label == "included":
                included += 1
            elif label == "not included":
                not_included += 1
            elif label == "excluded":
                excluded += 1
            elif label == "not excluded":
                not_excluded += 1
            elif label == "not enough information":
                not_enough += 1

    if excluded > 0:
        verdict = "EXCLUDED"
    elif not_included > 0:
        verdict = "UNLIKELY ELIGIBLE"
    elif included > 0 and not_included == 0 and excluded == 0:
        verdict = "LIKELY ELIGIBLE"
    else:
        verdict = "UNCERTAIN"

    return (
        f"{verdict} (inclusion: {included} met, {not_included} not met | "
        f"exclusion: {excluded} triggered, {not_excluded} clear | "
        f"{not_enough} insufficient info)"
    )


class EligibilityAgent:
    """Subscribes to TrialTopMatches, evaluates patient eligibility for each trial."""

    def __init__(self):
        self.name = "eligibility_agent"
        self.last_total_tokens = 0

    async def on_messages(self, messages, cancellation_token=None):
        content = messages[0].content if messages else ""

        try:
            data = json.loads(content)
            patient_description = data.get("patient_description", "")
            trials = data.get("trials", [])
        except (json.JSONDecodeError, KeyError):
            print(f"  [{self.name}] Could not parse TrialTopMatches payload")
            return Response(
                chat_message=TextMessage(
                    content="No trials to evaluate", source=self.name
                )
            )

        if not trials or not patient_description:
            print(f"  [{self.name}] Missing patient or trials, skipping.")
            return Response(
                chat_message=TextMessage(
                    content="No trials to evaluate", source=self.name
                )
            )

        # Include observations from the workflow record
        workflow_id = bus.current_workflow_id()
        observations = get_workflow_observations(workflow_id)
        full_description = patient_description
        if observations:
            full_description = f"{patient_description}\n\n{observations}"
            bus._log_audit(
                workflow_id,
                self.name,
                "TrialTopMatches",
                "observations",
                {"observations": observations},
            )

        patient_note = prepare_patient_note(full_description)

        model = bus.get_workflow_param("model", DEFAULT_MODEL)
        client, resolved_model = get_llm_client(model)
        print(
            f"  [{self.name}] Evaluating eligibility for {len(trials)} trials using {model}..."
        )
        all_results = []
        self.last_total_tokens = 0

        for trial in trials:
            nct_id = trial["nct_id"]
            print(f"    Evaluating {nct_id}: {trial.get('brief_title', '')[:60]}...")

            eligibility, tokens, llm_chats = evaluate_trial(
                trial,
                patient_note,
                model=model,
                client=client,
                resolved_model=resolved_model,
            )
            self.last_total_tokens += tokens

            # Send per-trial progress to WebSocket clients
            if ADMIN_DB_URL:
                try:
                    workflow_id = bus.current_workflow_id()
                    trial_idx = trials.index(trial) + 1
                    msg = f"Evaluated trial {trial_idx}/{len(trials)}: {nct_id}"
                    conn = psycopg2.connect(ADMIN_DB_URL)
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT pg_notify('workflow_updates', %s)",
                        (
                            json.dumps(
                                {
                                    "workflow_id": workflow_id,
                                    "agent_message": msg,
                                    "display_type": "result",
                                }
                            ),
                        ),
                    )
                    conn.commit()
                    cur.close()
                    conn.close()
                except Exception:
                    pass

            for chat in llm_chats:
                bus.schedule_broadcast(
                    AgentEvent(
                        message_type="EligibilityLlmChat",
                        content=json.dumps(chat),
                        workflow_id=bus.current_workflow_id(),
                    )
                )
            summary = summarize_eligibility(eligibility)

            all_results.append(
                {
                    "nct_id": nct_id,
                    "brief_title": trial.get("brief_title", ""),
                    "brief_summary": trial.get("brief_summary", ""),
                    "conditions": trial.get("conditions", ""),
                    "inclusion_criteria": trial.get("inclusion_criteria", ""),
                    "exclusion_criteria": trial.get("exclusion_criteria", ""),
                    "retrieval_score": trial.get("score", 0),
                    "eligibility": eligibility,
                    "summary": summary,
                }
            )

            print(f"      -> {summary}")

            # Save criteria to trial_eligibility if not already stored
            if ADMIN_DB_URL:
                try:
                    conn = psycopg2.connect(ADMIN_DB_URL)
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT 1 FROM trial_eligibility WHERE nct_id = %s LIMIT 1",
                        (nct_id,),
                    )
                    if not cur.fetchone():
                        for elig_type in ["inclusion", "exclusion"]:
                            criteria_text = trial.get(f"{elig_type}_criteria", "")
                            if criteria_text and len(criteria_text.strip()) >= 5:
                                cur.execute(
                                    "INSERT INTO trial_eligibility (nct_id, eligibility_type, criteria) VALUES (%s, %s, %s)",
                                    (nct_id, elig_type, criteria_text),
                                )
                        conn.commit()
                    cur.close()
                    conn.close()
                except Exception as e:
                    print(f"    Failed to save trial_eligibility for {nct_id}: {e}")

            # Save eligibility results to workflow_trial_eligibility
            if ADMIN_DB_URL:
                try:
                    workflow_id = bus.current_workflow_id()
                    conn = psycopg2.connect(ADMIN_DB_URL)
                    cur = conn.cursor()
                    for elig_type in ["inclusion", "exclusion"]:
                        criteria_dict = eligibility.get(elig_type, {})
                        if not isinstance(criteria_dict, dict):
                            continue
                        for criterion_num, values in criteria_dict.items():
                            if not isinstance(values, list) or len(values) < 3:
                                continue
                            reasoning = (
                                values[0]
                                if isinstance(values[0], str)
                                else " ".join(str(v) for v in values[0])
                            )
                            label = values[2]
                            cur.execute(
                                """INSERT INTO workflow_trial_eligibility
                                   (workflow_id, nct_id, eligibility_type, criterion_number, reasoning, eligibility_label)
                                   VALUES (%s, %s, %s, %s, %s, %s)""",
                                (
                                    workflow_id,
                                    nct_id,
                                    elig_type,
                                    int(criterion_num),
                                    reasoning,
                                    label,
                                ),
                            )
                    conn.commit()
                    cur.close()
                    conn.close()
                except Exception as e:
                    print(
                        f"    Failed to save workflow_trial_eligibility for {nct_id}: {e}"
                    )

        print(f"\n  === Eligibility Results ({len(all_results)} trials) ===")
        for i, r in enumerate(all_results, 1):
            print(f"  {i:>3}. [{r['nct_id']}] {r['brief_title'][:60]}")
            print(f"       {r['summary']}")

        result_text = json.dumps(all_results, indent=2)

        bus.schedule_broadcast(
            AgentEvent(
                message_type="EligibilityResults",
                content=result_text,
                workflow_id=bus.current_workflow_id(),
            )
        )

        return Response(chat_message=TextMessage(content=result_text, source=self.name))


eligibility_agent = EligibilityAgent()
bus.subscribe("TrialTopMatches", eligibility_agent)
