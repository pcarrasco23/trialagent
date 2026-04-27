"""
Agent that subscribes to PatientConditions events,
extracts ranked condition keywords, and broadcasts
them as TrialKeywords for downstream agents.
"""

import json
import os

from autogen_agentchat.base import Response
from autogen_agentchat.messages import TextMessage
from agents.event_bus import bus, AgentEvent
from agents.prompt_loader import get_prompt
from lib.llm_client import get_llm_client

DEFAULT_MODEL = os.environ.get("KEYWORD_MODEL", "gpt-4o")


class KeywordExtractionBusAgent:
    """Listens for PatientConditions, extracts keywords, broadcasts TrialKeywords."""

    def __init__(self):
        self.name = "keyword_extraction_agent"
        self.last_total_tokens = 0

    async def on_messages(self, messages, cancellation_token=None):
        content = messages[0].content if messages else ""

        # Filter conditions to only keep those with "(disorder)"
        filtered_lines = []
        for line in content.split("\n"):
            if line.startswith("- "):
                if "(disorder)" in line.lower():
                    filtered_lines.append(line)
            else:
                filtered_lines.append(line)
        content = "\n".join(filtered_lines)

        model = bus.get_workflow_param("model", DEFAULT_MODEL)
        client, resolved_model = get_llm_client(model)
        system_prompt = get_prompt("keyword_extraction_agent", "default", "system")
        user_template = get_prompt("keyword_extraction_agent", "default", "user")
        user_prompt = user_template.format(patient_description=content)

        response = client.chat.completions.create(
            model=resolved_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
        )

        self.last_total_tokens = response.usage.total_tokens if response.usage else 0
        raw_output = response.choices[0].message.content.strip()

        # Strip <think>...</think> tags (e.g. from Qwen models)
        import re
        raw_output = re.sub(r"<think>.*?</think>", "", raw_output, flags=re.DOTALL).strip()

        print(f"  [keyword_extraction_agent] Raw output: {raw_output}")

        # Broadcast the full LLM chat for tuning dataset collection
        llm_chat_event = AgentEvent(
            message_type="KeywordLlmChat",
            content=json.dumps(
                {
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                        {"role": "assistant", "content": raw_output},
                    ]
                }
            ),
            workflow_id=bus.current_workflow_id(),
        )
        bus.schedule_broadcast(llm_chat_event)

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
            content=json.dumps(
                {
                    "summary": summary,
                    "conditions": conditions,
                    "patient_description": content,
                }
            ),
            workflow_id=bus.current_workflow_id(),
        )
        bus.schedule_broadcast(keyword_event)

        output = json.dumps(
            {
                "summary": summary,
                "conditions": conditions,
            }
        )

        return Response(
            chat_message=TextMessage(
                content=output,
                source=self.name,
            )
        )


keyword_bus_agent = KeywordExtractionBusAgent()
bus.subscribe("PatientConditions", keyword_bus_agent)
