FROM node:20-slim AS frontend
WORKDIR /app
COPY clients/synthea/frontend/package.json clients/synthea/frontend/package-lock.json ./
RUN npm ci
COPY clients/synthea/frontend/ .
RUN npm run build

FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir psycopg2-binary fastapi uvicorn httpx websockets
COPY clients/synthea/ ./synthea/
COPY --from=frontend /app/dist ./synthea/frontend/dist
CMD ["uvicorn", "synthea.api:app", "--host", "0.0.0.0", "--port", "8001"]
