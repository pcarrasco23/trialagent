"""
Seed prompts into the database from data/seed_prompts.json.

Only inserts prompts that don't already exist (by agent_name, prompt_key, prompt_type).
Existing prompts are not modified.

Usage:
    python scripts/seed_prompts.py

Requires: ADMIN_DB_URL environment variable.
"""

import json
import os
from pathlib import Path

import psycopg2

ADMIN_DB_URL = os.environ.get("ADMIN_DB_URL", "")
SEED_PROMPTS_PATH = Path(__file__).parent.parent / "data" / "seed_prompts.json"


def main():
    if not ADMIN_DB_URL:
        print("Error: ADMIN_DB_URL environment variable is required")
        return

    if not SEED_PROMPTS_PATH.exists():
        print(f"Error: {SEED_PROMPTS_PATH} not found")
        return

    with open(SEED_PROMPTS_PATH) as f:
        prompts = json.load(f)

    conn = psycopg2.connect(ADMIN_DB_URL)
    conn.autocommit = True
    cur = conn.cursor()

    for prompt in prompts:
        cur.execute(
            """INSERT INTO prompts (agent_name, prompt_key, prompt_type, prompt_text, description)
               VALUES (%s, %s, %s, %s, %s)""",
            (prompt["agent_name"], prompt["prompt_key"], prompt["prompt_type"],
             prompt["prompt_text"], prompt["description"]),
        )

    cur.close()
    conn.close()
    print(f"Seeded {len(prompts)} prompts.")


if __name__ == "__main__":
    main()
