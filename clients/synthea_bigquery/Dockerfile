FROM node:20-slim AS frontend
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ .
RUN npm run build

FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn google-cloud-bigquery httpx websockets
COPY . ./synthea_bigquery/
COPY --from=frontend /app/dist ./synthea_bigquery/frontend/dist
CMD ["uvicorn", "synthea_bigquery.api:app", "--host", "0.0.0.0", "--port", "8003"]
