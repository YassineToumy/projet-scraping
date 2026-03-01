#!/bin/bash
set -e

echo "══════════════════════════════════════════════════"
echo "🚀 Scraper Container Starting"
echo "   Time: $(date -u)"
echo "══════════════════════════════════════════════════"

# ── Export all env vars so cron jobs can access them ──
printenv | grep -v "no_proxy" >> /etc/environment

# ── Write crontab with env vars baked in ──
cat > /etc/cron.d/scrapers <<'CRON'
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin

# Bien'ici (France) — 03:00 UTC
0 3 * * * root /app/runner.sh bienici >> /app/logs/cron.log 2>&1

# Mubawab (Tunisia) — 04:00 UTC
0 4 * * * root /app/runner.sh mubawab >> /app/logs/cron.log 2>&1

# MktList (Canada) — 05:00 UTC
0 5 * * * root /app/runner.sh mktlist >> /app/logs/cron.log 2>&1

# PropertyFinder (Egypt) — 06:00 UTC
0 6 * * * root /app/runner.sh propertyfinder >> /app/logs/cron.log 2>&1

# Sync MongoDB -> PostgreSQL — 10:00 UTC
0 10 * * * root /app/runner.sh sync >> /app/logs/cron.log 2>&1

# Health monitor — every 6 hours
0 */6 * * * root /app/runner.sh monitor >> /app/logs/cron.log 2>&1

CRON

chmod 0644 /etc/cron.d/scrapers
crontab /etc/cron.d/scrapers

echo "✅ Cron schedule installed:"
crontab -l
echo ""

# ── Verify connections ──
echo "🔌 Testing MongoDB..."
python -c "
from pymongo import MongoClient
import os
c = MongoClient(os.environ['MONGODB_URI'], serverSelectionTimeoutMS=5000)
c.admin.command('ping')
print('  ✅ MongoDB OK')
c.close()
" || echo "  ❌ MongoDB connection failed"

echo "🔌 Testing PostgreSQL..."
python -c "
import psycopg2, os
conn = psycopg2.connect(os.environ['POSTGRES_DSN'])
cur = conn.cursor()
cur.execute('SELECT 1')
print('  ✅ PostgreSQL OK')
conn.close()
" || echo "  ❌ PostgreSQL connection failed"

echo ""
echo "══════════════════════════════════════════════════"
echo "✅ Ready — cron daemon starting"
echo "   Logs: /app/logs/"
echo "══════════════════════════════════════════════════"

# ── Start cron in foreground (keeps container alive) ──
exec cron -f