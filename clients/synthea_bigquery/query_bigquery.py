"""
Run a query against Google BigQuery and print the results.

Requires:
    - google-cloud-bigquery
    - GOOGLE_APPLICATION_CREDENTIALS env var pointing to a service account JSON key,
      or authenticated via `gcloud auth application-default login`

Usage:
    python mimic/query_bigquery.py "SELECT * FROM my_dataset.my_table LIMIT 10"
    python mimic/query_bigquery.py --project my-project-id "SELECT 1"
"""

import argparse
import os

from google.cloud import bigquery


def run_query(query: str, project: str | None = None):
    client = bigquery.Client(project=project)
    print(f"Running query against project: {client.project}")
    print(f"Query: {query}\n")

    results = client.query(query).result()

    fields = [field.name for field in results.schema]
    print("\t".join(fields))
    print("\t".join(["---"] * len(fields)))

    row_count = 0
    for row in results:
        print("\t".join(str(row[f]) for f in fields))
        row_count += 1

    print(f"\n{row_count} rows returned.")


def main():
    parser = argparse.ArgumentParser(description="Run a BigQuery query")
    parser.add_argument("query", help="SQL query to execute")
    parser.add_argument(
        "--project",
        default=os.environ.get("GOOGLE_CLOUD_PROJECT"),
        help="Google Cloud project ID (default: GOOGLE_CLOUD_PROJECT env var)",
    )
    args = parser.parse_args()

    run_query(args.query, project=args.project)


if __name__ == "__main__":
    main()
