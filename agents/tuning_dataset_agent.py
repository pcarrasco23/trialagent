"""
Agent that collects LLM chat messages for fine-tuning dataset generation.

Subscribes to:
    - KeywordLlmChat (keyword extraction LLM input/output)
    - EligibilityLlmChat (eligibility evaluation LLM input/output)
"""

import json
import os

import psycopg2
from autogen_agentchat.base import Response
from autogen_agentchat.messages import TextMessage

from agents.event_bus import bus

ADMIN_DB_URL = os.environ.get("ADMIN_DB_URL", "")


class TuningDatasetAgent:
    """Collects LLM chat messages and saves them to the tuning_dataset table."""

    def __init__(self):
        self.name = "tuning_dataset_agent"

    async def on_messages(self, messages, cancellation_token=None):
        content = messages[0].content if messages else ""

        try:
            data = json.loads(content)
        except (json.JSONDecodeError, KeyError):
            data = {"messages": content}

        print(f"  [{self.name}] Saving tuning record")

        if ADMIN_DB_URL:
            try:
                conn = psycopg2.connect(ADMIN_DB_URL)
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO tuning_dataset (message) VALUES (%s)",
                    (json.dumps(data),),
                )
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                print(f"  [{self.name}] Failed to save: {e}")

        return Response(
            chat_message=TextMessage(content="saved", source=self.name)
        )


tuning_dataset_agent = TuningDatasetAgent()
bus.subscribe("KeywordLlmChat", tuning_dataset_agent)
bus.subscribe("EligibilityLlmChat", tuning_dataset_agent)
