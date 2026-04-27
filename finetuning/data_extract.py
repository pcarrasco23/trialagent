"""
Extract training data from the tuning_dataset table to a JSONL file.

Queries tuning_dataset for all records where processed_at IS NULL,
writes them to a JSONL file, and marks them as processed.

Usage:
    python finetuning/data_extract.py

Requires:
    pip install psycopg2-binary
    ADMIN_DB_URL environment variable
"""

import json
import os
from datetime import datetime
from pathlib import Path

import psycopg2

ADMIN_DB_URL = os.environ.get("ADMIN_DB_URL", "")


def fetch_training_data(db_url: str) -> list[dict]:
    """Fetch unprocessed records from tuning_dataset."""
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, message FROM tuning_dataset WHERE processed_at IS NULL ORDER BY id"
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"id": row[0], "message": row[1]} for row in rows]


def mark_processed(db_url: str, record_ids: list[int]):
    """Mark records as processed."""
    if not record_ids:
        return
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute(
        "UPDATE tuning_dataset SET processed_at = %s WHERE id = ANY(%s)",
        (datetime.now(), record_ids),
    )
    conn.commit()
    cur.close()
    conn.close()


def write_training_file(records: list[dict], data_dir: Path) -> int:
    """Write records to train.jsonl. Returns count written."""
    count = 0
    with open(data_dir / "train.jsonl", "w") as f:
        for record in records:
            message = record["message"]
            if isinstance(message, str):
                message = json.loads(message)
            if "messages" in message:
                f.write(json.dumps(message) + "\n")
                count += 1
    return count


def main():
    if not ADMIN_DB_URL:
        print("Error: ADMIN_DB_URL environment variable is required")
        return

    print("Fetching unprocessed training data...")
    records = fetch_training_data(ADMIN_DB_URL)
    if not records:
        print("No unprocessed records found.")
        return
    print(f"  Found {len(records)} unprocessed records")

    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    count = write_training_file(records, data_dir)
    print(f"  Wrote {count} training examples to {data_dir / 'train.jsonl'}")

    if count > 0:
        record_ids = [r["id"] for r in records]
        mark_processed(ADMIN_DB_URL, record_ids)
        print(f"  Marked {len(record_ids)} records as processed.")

    print("Done.")


if __name__ == "__main__":
    main()
