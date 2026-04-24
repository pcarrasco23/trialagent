"""
Entrypoint for the db-setup container.
Waits for both Postgres instances, then runs setup only if needed.
"""

import os
import subprocess
import sys
import time

import psycopg2

ADMIN_DB_URL = os.environ["ADMIN_DB_URL"]
SYNTHEA_FHIR_DB_URL = os.environ["SYNTHEA_FHIR_DB_URL"]


def wait_for_db(url, name):
    print(f"Waiting for {name}...")
    while True:
        try:
            conn = psycopg2.connect(url)
            conn.close()
            print(f"  {name} is ready.")
            return
        except Exception:
            time.sleep(1)


def table_has_rows(url, table):
    try:
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        cur.execute(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
            (table,),
        )
        if not cur.fetchone()[0]:
            conn.close()
            return False
        cur.execute(f"SELECT EXISTS(SELECT 1 FROM {table} LIMIT 1)")
        result = cur.fetchone()[0]
        conn.close()
        return result
    except Exception:
        return False


def run(cmd):
    result = subprocess.run([sys.executable] + cmd)
    if result.returncode != 0:
        sys.exit(result.returncode)


def main():
    wait_for_db(ADMIN_DB_URL, "postgres-trial-agent")
    wait_for_db(SYNTHEA_FHIR_DB_URL, "postgres-synthea")

    print("Running admin database setup...")
    run(["scripts/admin_db_setup.py"])

    if table_has_rows(ADMIN_DB_URL, "prompts"):
        print("Prompts already seeded, skipping.")
    else:
        print("Seeding prompts...")
        run(["scripts/seed_prompts.py"])

    print("Running synthea schema setup...")
    run(["clients/synthea/synthea_fhir_postgres_import.py", "--schema-only"])

    if table_has_rows(SYNTHEA_FHIR_DB_URL, "patient"):
        print("FHIR data already imported, skipping.")
    else:
        print("Running FHIR import...")
        run(["clients/synthea/synthea_fhir_postgres_import.py"])

    print("Database setup complete.")


if __name__ == "__main__":
    main()
