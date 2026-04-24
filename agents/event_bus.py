import json
import os
from dataclasses import dataclass

import psycopg2
from autogen_agentchat.messages import TextMessage
from psycopg2.extras import Json


@dataclass
class AgentEvent:
    message_type: str
    content: str
    source: str = "kafka"
    workflow_id: str = ""


class EventBus:
    def __init__(self, admin_db_url: str | None = None):
        self._subscribers: dict[str, list] = {}
        self._admin_db_url = admin_db_url
        self._pending_events: list[AgentEvent] = []
        self._current_workflow_id = ""
        self._workflow_params: dict = {}

    def subscribe(self, message_type: str, agent):
        self._subscribers.setdefault(message_type, []).append(agent)
        print(f"  [{agent.name}] subscribed to '{message_type}'")

    def _log_audit(
        self,
        workflow_id: str,
        agent_name: str,
        message_type: str,
        audit_type: str,
        payload,
        total_tokens: int | None = None,
    ):
        if not self._admin_db_url:
            return
        try:
            conn = psycopg2.connect(self._admin_db_url)
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO audits (workflow_id, agent_name, message_type,
                   audit_type, payload, total_tokens)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (
                    workflow_id,
                    agent_name,
                    message_type,
                    audit_type,
                    Json(payload),
                    total_tokens,
                ),
            )
            notify_payload = {
                "workflow_id": workflow_id,
                "agent_name": agent_name,
                "audit_type": audit_type,
            }
            if agent_name == "trial_search_agent":
                notify_payload["trial_corpus"] = self._workflow_params.get(
                    "trial_corpus", "clinical_trials_gov"
                )
            cur.execute(
                "SELECT pg_notify('workflow_updates', %s)",
                (json.dumps(notify_payload),),
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"  [audit] Failed to log: {e}")

    def schedule_broadcast(self, event: AgentEvent):
        """Queue a child event to be broadcast after the current agent's output is logged."""
        self._pending_events.append(event)

    async def broadcast(self, event: AgentEvent):
        agents = self._subscribers.get(event.message_type, [])
        if not agents:
            print(f"  No subscribers for message type '{event.message_type}'")
            return

        self._current_workflow_id = event.workflow_id

        message = TextMessage(content=event.content, source=event.source)

        for agent in agents:
            # Skip qrels_agent audit if no qrels on this workflow
            skip_audit = False
            if agent.name == "qrels_agent":
                from lib.workflow import get_workflow_qrels

                if not get_workflow_qrels(event.workflow_id):
                    skip_audit = True

            # Log input
            if not skip_audit:
                self._log_audit(
                    event.workflow_id,
                    agent.name,
                    event.message_type,
                    "input",
                    {"content": event.content},
                )

            # Clear pending events before calling the agent
            self._pending_events = []

            response = await agent.on_messages([message], cancellation_token=None)

            # Log output BEFORE dispatching child events
            output_content = (
                response.chat_message.content
                if response and response.chat_message
                else ""
            )
            total_tokens = getattr(agent, "last_total_tokens", None)
            if not skip_audit:
                self._log_audit(
                    event.workflow_id,
                    agent.name,
                    event.message_type,
                    "output",
                    {"content": output_content},
                    total_tokens=total_tokens,
                )
            print(f"  [{agent.name}] response: {output_content[:200]}")

            # Send extracted keywords to WebSocket clients
            if agent.name == "keyword_extraction_agent" and output_content:
                try:
                    parsed = json.loads(output_content)
                    conditions = parsed.get("conditions", [])
                    if conditions and self._admin_db_url:
                        keywords_msg = "Keywords: " + ", ".join(conditions[:10])
                        if len(conditions) > 10:
                            keywords_msg += f" (+{len(conditions) - 10} more)"
                        conn2 = psycopg2.connect(self._admin_db_url)
                        cur2 = conn2.cursor()
                        cur2.execute(
                            "SELECT pg_notify('workflow_updates', %s)",
                            (
                                json.dumps(
                                    {
                                        "workflow_id": event.workflow_id,
                                        "agent_message": keywords_msg,
                                        "display_type": "result",
                                    }
                                ),
                            ),
                        )
                        conn2.commit()
                        cur2.close()
                        conn2.close()
                except Exception:
                    pass

            # Send matched trial IDs to WebSocket clients
            if agent.name == "trial_search_agent" and output_content:
                try:
                    # output_content is like "Found 5 matching trials. Top matches: NCT123, NCT456, NCT789"
                    # Parse the trial IDs from the broadcast event content instead
                    for pending_evt in self._pending_events:
                        if pending_evt.message_type == "TrialTopMatches":
                            matches = json.loads(pending_evt.content)
                            trials = matches.get("trials", [])
                            nct_ids = [t["nct_id"] for t in trials]
                            if nct_ids and self._admin_db_url:
                                trials_msg = (
                                    f"Matched {len(nct_ids)} trials: "
                                    + ", ".join(nct_ids[:10])
                                )
                                if len(nct_ids) > 10:
                                    trials_msg += f" (+{len(nct_ids) - 10} more)"
                                conn2 = psycopg2.connect(self._admin_db_url)
                                cur2 = conn2.cursor()
                                cur2.execute(
                                    "SELECT pg_notify('workflow_updates', %s)",
                                    (
                                        json.dumps(
                                            {
                                                "workflow_id": event.workflow_id,
                                                "agent_message": trials_msg,
                                                "display_type": "result",
                                            }
                                        ),
                                    ),
                                )
                                conn2.commit()
                                cur2.close()
                                conn2.close()
                            break
                except Exception:
                    pass

            # Send eligibility summaries to WebSocket clients
            if agent.name == "eligibility_agent" and output_content:
                try:
                    results = json.loads(output_content)
                    summaries = [
                        f"{r['nct_id']}: {r['summary']}"
                        for r in results
                        if "nct_id" in r and "summary" in r
                    ]
                    if summaries and self._admin_db_url:
                        elig_msg = "Eligibility results:\n" + "\n".join(summaries)
                        conn2 = psycopg2.connect(self._admin_db_url)
                        cur2 = conn2.cursor()
                        cur2.execute(
                            "SELECT pg_notify('workflow_updates', %s)",
                            (
                                json.dumps(
                                    {
                                        "workflow_id": event.workflow_id,
                                        "agent_message": elig_msg,
                                        "display_type": "result",
                                    }
                                ),
                            ),
                        )
                        conn2.commit()
                        cur2.close()
                        conn2.close()
                except Exception:
                    pass

            # Send qrels evaluation results to WebSocket clients
            if (
                agent.name == "qrels_agent"
                and output_content
                and output_content != "No qrels available"
            ):
                try:
                    metrics = json.loads(output_content)
                    lines = [f"{k}: {v:.4f}" for k, v in sorted(metrics.items())]
                    if lines and self._admin_db_url:
                        qrels_msg = "Evaluation metrics:\n" + "\n".join(lines)
                        conn2 = psycopg2.connect(self._admin_db_url)
                        cur2 = conn2.cursor()
                        cur2.execute(
                            "SELECT pg_notify('workflow_updates', %s)",
                            (
                                json.dumps(
                                    {
                                        "workflow_id": event.workflow_id,
                                        "agent_message": qrels_msg,
                                        "display_type": "result",
                                    }
                                ),
                            ),
                        )
                        conn2.commit()
                        cur2.close()
                        conn2.close()
                except Exception:
                    pass

            # Now dispatch any child events the agent queued
            pending = list(self._pending_events)
            self._pending_events = []
            for child_event in pending:
                await self.broadcast(child_event)

    def set_workflow_params(self, params: dict):
        self._workflow_params = params

    def get_workflow_param(self, key: str, default=None):
        return self._workflow_params.get(key, default)

    def current_workflow_id(self) -> str:
        return self._current_workflow_id


# Singleton bus
ADMIN_DB_URL = os.environ.get("ADMIN_DB_URL", "")
bus = EventBus(admin_db_url=ADMIN_DB_URL)
