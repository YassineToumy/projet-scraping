#!/bin/bash
# ============================================================
# Runs a scraper script inside the container
# Usage: ./runner.sh <bienici|mubawab|mktlist|propertyfinder|sync|monitor>
# ============================================================

set -uo pipefail

# Load env vars (cron doesn't inherit them)
set -a
source /etc/environment
set +a

SCRAPER="$1"
TIMESTAMP=$(date -u +"%Y-%m-%d_%H-%M-%S")
LOGFILE="/app/logs/${SCRAPER}_${TIMESTAMP}.log"

echo "══════════════════════════════════════════════════"
echo "🚀 [$TIMESTAMP] Starting: ${SCRAPER}"
echo "══════════════════════════════════════════════════"

# Check if already running (prevent overlap)
PIDFILE="/tmp/${SCRAPER}.pid"
if [ -f "$PIDFILE" ] && kill -0 "$(cat $PIDFILE)" 2>/dev/null; then
    echo "⚠️  ${SCRAPER} already running (PID $(cat $PIDFILE)) — skip"
    exit 0
fi

cd /app

case "$SCRAPER" in
    bienici)
        python -u bienici_scraper_locations.py > "$LOGFILE" 2>&1 &
        ;;
    mubawab)
        python -u scraper_mubawab_locations.py > "$LOGFILE" 2>&1 &
        ;;
    mktlist)
        python -u scraper_mktlist_locations.py > "$LOGFILE" 2>&1 &
        ;;
    propertyfinder)
        python -u scraper_propertyfinder_locations.py > "$LOGFILE" 2>&1 &
        ;;
    sync)
        python -u mongo_to_postgre.py --once > "$LOGFILE" 2>&1 &
        ;;
    monitor)
        python -u monitor.py > "$LOGFILE" 2>&1 &
        ;;
    *)
        echo "❌ Unknown: ${SCRAPER}"
        exit 1
        ;;
esac

PID=$!
echo $PID > "$PIDFILE"
echo "Started PID ${PID} — log: ${LOGFILE}"

# Wait for completion
wait $PID
EXIT_CODE=$?
rm -f "$PIDFILE"

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ ${SCRAPER} done ($(date -u))"
else
    echo "❌ ${SCRAPER} failed (exit ${EXIT_CODE})"
fi

# Rotate logs older than 30 days
find /app/logs -name "${SCRAPER}_*.log" -mtime +30 -delete 2>/dev/null || true

echo "Exit: ${EXIT_CODE} | Duration: ${SECONDS}s"