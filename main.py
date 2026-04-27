"""
Entry point for the TrialRT API server (event bus + agents).

Required environment variables:
    ADMIN_DB_URL   - PostgreSQL URL for the admin database (trial info, audits, prompts)
    OPENAI_API_KEY - OpenAI API key
    QDRANT_URL     - Qdrant URL (default: http://localhost:6333)

Run the synthea CDC consumer separately:
    python synthea/condition_cdc_consumer.py

Usage:
    python main.py
"""

import uvicorn

if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000)
