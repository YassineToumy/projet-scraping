#!/bin/bash
# ============================================================
# Starts a scraper container via docker compose profiles
# Usage: ./scheduler.sh <bienici|mubawab|mktlist|propertyfinder|monitor>
# ============================================================

set -uo pipefail

SCRAPER="$1"
TIMESTAMP=$(date -u +"%Y-%m-%d_%H-%M-%S")
LOGFILE="/app/logs/${SCRAPER}_${TIMESTAMP}.log"
COMPOSE_FILE="/app/docker-compose.scrapers.yml"
CONTAINER="scraper-${SCRAPER}"
MAX_RUNTIME=14400  # 4h timeout

echo "══════════════════════════════════════════════════"
echo "🚀 [$TIMESTAMP] Starting: ${SCRAPER}"
echo "══════════════════════════════════════════════════"

# Prevent overlapping runs
if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "⚠️  ${CONTAINER} already running — skip"
    exit 0
fi

# Clean up any stopped container with same name
docker rm -f "${CONTAINER}" 2>/dev/null || true

# Run the scraper (blocking — waits for completion)
timeout ${MAX_RUNTIME} docker compose \
    -f "${COMPOSE_FILE}" \
    --profile "${SCRAPER}" \
    run --rm --name "${CONTAINER}" \
    "${CONTAINER}" \
    > "${LOGFILE}" 2>&1

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ ${SCRAPER} done ($(date -u))"
elif [ $EXIT_CODE -eq 124 ]; then
    echo "⏰ ${SCRAPER} TIMEOUT after ${MAX_RUNTIME}s"
    docker stop "${CONTAINER}" 2>/dev/null || true
else
    echo "❌ ${SCRAPER} failed (exit ${EXIT_CODE})"
fi

# Cleanup
docker rm "${CONTAINER}" 2>/dev/null || true

# Rotate logs older than 30 days
find /app/logs -name "${SCRAPER}_*.log" -mtime +30 -delete 2>/dev/null || true

echo "Exit: ${EXIT_CODE} | Log: ${LOGFILE}"