"""
Returns an OpenAI-compatible client and resolved model name based on the
requested model.  Models served by Ollama are routed to the Ollama
OpenAI-compatible endpoint; everything else goes to OpenAI.
"""

import os

from openai import OpenAI

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434/v1")

# Map of UI model names → Ollama model tags
OLLAMA_MODELS = {
    "meditron": "meditron",
}

_openai_client = None
_ollama_client = None


def get_llm_client(model: str) -> tuple["OpenAI", str]:
    """Return (client, resolved_model) for the given model name."""
    global _openai_client, _ollama_client

    if model in OLLAMA_MODELS:
        if _ollama_client is None:
            _ollama_client = OpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")
        return _ollama_client, OLLAMA_MODELS[model]

    if _openai_client is None:
        _openai_client = OpenAI(api_key=OPENAI_API_KEY)
    return _openai_client, model
