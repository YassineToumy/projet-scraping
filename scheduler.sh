#!/bin/bash
# ============================================================
# Triggers a scraper by running docker run with the built image
# Usage: ./scheduler.sh <bienici|mubawab|mktlist|propertyfinder|sync|monitor>
# ============================================================

set -uo pipefail

SCRAPER="$1"
TIMESTAMP=$(date -u +"%Y-%m-%d_%H-%M-%S")
LOGFILE="/app/logs/${SCRAPER}_${TIMESTAMP}.log"
CONTAINER="scraper-${SCRAPER}"
MAX_RUNTIME=14400  # 4h

echo "══════════════════════════════════════════════════"
echo "🚀 [$TIMESTAMP] Starting: ${SCRAPER}"
echo "══════════════════════════════════════════════════"

# Prevent overlapping runs
if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}-run$"; then
    echo "⚠️  ${CONTAINER} already running — skip"
    exit 0
fi

# Clean up old run container
docker rm -f "${CONTAINER}-run" 2>/dev/null || true

# Determine the command based on scraper name
case "$SCRAPER" in
    bienici)
        CMD="python -u bienici_scraper_locations.py"
        ;;
    mubawab)
        CMD="python -u scraper_mubawab_locations.py"
        ;;
    mktlist)
        CMD="python -u scraper_mktlist_locations.py"
        ;;
    propertyfinder)
        CMD="python -u scraper_propertyfinder_locations.py"
        ;;
    sync)
        CMD="python -u mongo_to_postgre.py --once"
        ;;
    monitor)
        CMD="python -u monitor.py"
        ;;
    *)
        echo "❌ Unknown scraper: ${SCRAPER}"
        exit 1
        ;;
esac

# Get the image name from the existing container
IMAGE=$(docker inspect --format='{{.Config.Image}}' "${CONTAINER}" 2>/dev/null)

if [ -z "$IMAGE" ]; then
    echo "❌ No image found for ${CONTAINER} — was it built?"
    exit 1
fi

# Run the scraper using the pre-built image
# Copy volumes and env from the compose service container
timeout ${MAX_RUNTIME} docker run --rm \
    --name "${CONTAINER}-run" \
    --env-file /app/.env \
    --network host \
    -v /app/logs:/app/logs \
    "${IMAGE}" \
    ${CMD} \
    > "${LOGFILE}" 2>&1

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ ${SCRAPER} done ($(date -u))"
elif [ $EXIT_CODE -eq 124 ]; then
    echo "⏰ ${SCRAPER} TIMEOUT after ${MAX_RUNTIME}s"
    docker stop "${CONTAINER}-run" 2>/dev/null || true
else
    echo "❌ ${SCRAPER} failed (exit ${EXIT_CODE})"
fi

# Rotate logs older than 30 days
find /app/logs -name "${SCRAPER}_*.log" -mtime +30 -delete 2>/dev/null || true

echo "Exit: ${EXIT_CODE} | Log: ${LOGFILE}"