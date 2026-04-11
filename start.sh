#!/usr/bin/env bash
# start.sh — Start the full Personal Opportunity Graph stack
set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"

# ── 1. Docker services ─────────────────────────────────────────────────────
echo "Starting Docker services (Neo4j, Qdrant, Ollama)..."
docker-compose up -d

# ── 2. Wait for Neo4j ──────────────────────────────────────────────────────
echo "Waiting for Neo4j to be ready..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:7474 > /dev/null 2>&1; then
    echo "  Neo4j ready."
    break
  fi
  printf "."
  sleep 2
done

# ── 3. Pull Ollama models (idempotent) ─────────────────────────────────────
echo "Pulling Ollama models (nomic-embed-text, llama3)..."
docker exec pog-ollama ollama pull nomic-embed-text 2>&1 | tail -3 &
docker exec pog-ollama ollama pull llama3 2>&1 | tail -3 &
wait

# ── 4. Backend ─────────────────────────────────────────────────────────────
echo "Starting FastAPI backend on port 8001..."
cd "$ROOT"
if [ ! -d ".venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv .venv
  .venv/bin/pip install --quiet -r requirements.txt
fi

.venv/bin/uvicorn backend.main:app --host 0.0.0.0 --port 8001 --reload &
BACKEND_PID=$!
echo "  Backend PID: $BACKEND_PID"

# ── 5. Frontend ────────────────────────────────────────────────────────────
echo "Starting frontend dev server on port 5173..."
cd "$ROOT/frontend"
if [ ! -d "node_modules" ]; then
  echo "Installing frontend dependencies..."
  npm install
fi
npm run dev &
FRONTEND_PID=$!
echo "  Frontend PID: $FRONTEND_PID"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Personal Opportunity Graph is running!"
echo ""
echo "  Frontend:  http://localhost:5173"
echo "  API:       http://localhost:8001"
echo "  API docs:  http://localhost:8001/docs"
echo "  Neo4j:     http://localhost:7474"
echo "  Qdrant:    http://localhost:6333/dashboard"
echo ""
echo "  Press Ctrl+C to stop all services."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Wait and clean up on exit
cleanup() {
  echo ""
  echo "Stopping services..."
  kill $BACKEND_PID $FRONTEND_PID 2>/dev/null || true
  docker-compose stop
  exit 0
}
trap cleanup INT TERM

wait $BACKEND_PID
