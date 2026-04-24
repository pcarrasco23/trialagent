"""
Loads prompts from the trial_agent_admin database.
Caches prompts in memory after first load.
"""

import os
import psycopg2

ADMIN_DB_URL = os.environ.get("ADMIN_DB_URL", "")

_cache: dict[tuple[str, str, str], str] = {}


def get_prompt(agent_name: str, prompt_key: str, prompt_type: str) -> str:
    """Fetch a prompt from the database by agent_name, prompt_key, and prompt_type.
    Returns cached value if available."""
    cache_key = (agent_name, prompt_key, prompt_type)
    if cache_key in _cache:
        return _cache[cache_key]

    if not ADMIN_DB_URL:
        raise RuntimeError("ADMIN_DB_URL environment variable is required")

    conn = psycopg2.connect(ADMIN_DB_URL)
    cur = conn.cursor()
    cur.execute(
        """SELECT prompt_text FROM prompts
           WHERE agent_name = %s AND prompt_key = %s AND prompt_type = %s
           AND is_active = TRUE""",
        (agent_name, prompt_key, prompt_type),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise ValueError(
            f"Prompt not found: agent_name={agent_name}, "
            f"prompt_key={prompt_key}, prompt_type={prompt_type}"
        )

    _cache[cache_key] = row[0]
    return row[0]


def clear_cache():
    """Clear the prompt cache to force reloading from database."""
    _cache.clear()
