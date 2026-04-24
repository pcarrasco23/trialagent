FROM node:20-slim AS frontend
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ .
RUN npm run build

FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc build-essential && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir fastapi uvicorn httpx websockets pytrec-eval-terrier
COPY . ./trec/
COPY --from=frontend /app/dist ./trec/frontend/dist
CMD ["uvicorn", "trec.api:app", "--host", "0.0.0.0", "--port", "8004"]
