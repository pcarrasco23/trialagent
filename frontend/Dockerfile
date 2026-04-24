FROM node:20-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn httpx websockets
COPY --from=build /app/dist ./dist
COPY server.py .
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8002"]
