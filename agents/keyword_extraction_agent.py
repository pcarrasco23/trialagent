"""
Agent that subscribes to PatientConditions events,
extracts ranked condition keywords, and broadcasts
them as TrialKeywords for downstream agents.
"""

import json
import os

from autogen_agentchat.base import Response
from autogen_agentchat.messages import TextMessage
from openai import OpenAI

from agents.event_bus import bus, AgentEvent
from agents.prompt_loader import get_prompt

openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
DEFAULT_MODEL = os.environ.get("KEYWORD_MODEL", "gpt-4o")


class KeywordExtractionBusAgent:
    """Listens for PatientConditions, extracts keywords, broadcasts TrialKeywords."""

    def __init__(self):
        self.name = "keyword_extraction_agent"
        self.last_total_tokens = 0

    async def on_messages(self, messages, cancellation_token=None):
        content = messages[0].content if messages else ""

        model = bus.get_workflow_param("model", DEFAULT_MODEL)
        system_prompt = get_prompt("keyword_extraction_agent", "default", "system")
        user_template = get_prompt("keyword_extraction_agent", "default", "user")
        user_prompt = user_template.format(patient_description=content)

        response = openai_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
        )

        self.last_total_tokens = response.usage.total_tokens if response.usage else 0
        raw_output = response.choices[0].message.content.strip()
        print(f"  [keyword_extraction_agent] Raw output: {raw_output}")

        # Parse the JSON output
        try:
            text = raw_output
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            parsed = json.loads(text)
            conditions = parsed.get("conditions", [])
            summary = parsed.get("summary", "")
        except (json.JSONDecodeError, KeyError):
            print("  [keyword_extraction_agent] Failed to parse, using raw conditions")
            conditions = [
                line.strip("- ").split("(")[0].strip()
                for line in content.split("\n")
                if line.startswith("- ")
            ]
            summary = ""

        print(f"  [keyword_extraction_agent] Summary: {summary}")
        print(f"  [keyword_extraction_agent] Keywords ({len(conditions)}):")
        for c in conditions:
            print(f"    - {c}")

        # Broadcast keywords for the trial search agent
        keyword_event = AgentEvent(
            message_type="TrialKeywords",
            content=json.dumps({
                "summary": summary,
                "conditions": conditions,
                "patient_description": content,
            }),
            workflow_id=bus.current_workflow_id(),
        )
        bus.schedule_broadcast(keyword_event)

        output = json.dumps({
            "summary": summary,
            "conditions": conditions,
        })

        return Response(
            chat_message=TextMessage(
                content=output,
                source=self.name,
            )
        )


keyword_bus_agent = KeywordExtractionBusAgent()
bus.subscribe("PatientConditions", keyword_bus_agent)
