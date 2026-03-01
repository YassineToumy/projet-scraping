#!/bin/bash
set -e

echo "══════════════════════════════════════════════════"
echo "🚀 Scraper Container Starting"
echo "   Time: $(date -u)"
echo "══════════════════════════════════════════════════"

# ── Export all env vars so cron jobs can access them ──
printenv | grep -v "no_proxy" >> /etc/environment

# ── Write crontab ──
# PropertyFinder runs every 6h (slow, needs multiple runs)
# Others run once daily (fast HTTP scrapers)
cat > /etc/cron.d/scrapers <<'CRON'
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin

# Bien'ici (France) — daily 03:00 UTC (HTTP, fast)
0 3 * * * root /app/runner.sh bienici >> /app/logs/cron.log 2>&1

# Mubawab (Tunisia) — daily 04:00 UTC (HTTP, fast)
0 4 * * * root /app/runner.sh mubawab >> /app/logs/cron.log 2>&1

# MktList (Canada) — daily 05:00 UTC (HTTP, fast)
0 5 * * * root /app/runner.sh mktlist >> /app/logs/cron.log 2>&1

# PropertyFinder (Egypt) — every 6h (Playwright, slow, needs multiple passes)
0 */6 * * * root /app/runner.sh propertyfinder >> /app/logs/cron.log 2>&1

# Sync MongoDB -> PostgreSQL — daily 10:00 UTC
0 10 * * * root /app/runner.sh sync >> /app/logs/cron.log 2>&1

# Health monitor — every 6 hours
30 */6 * * * root /app/runner.sh monitor >> /app/logs/cron.log 2>&1

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
echo "📅 Schedule:"
echo "   03:00  🇫🇷 Bien'ici (daily)"
echo "   04:00  🇹🇳 Mubawab (daily)"
echo "   05:00  🇨🇦 MktList (daily)"
echo "   */6h   🇪🇬 PropertyFinder (every 6h)"
echo "   10:00  🔄 Sync to PostgreSQL (daily)"
echo "   */6h   📊 Health monitor"
echo ""
echo "✅ Ready — cron daemon starting"
echo "══════════════════════════════════════════════════"

# ── Run all scrapers once on startup to catch up ──
echo ""
echo "🔄 Running initial scrape on startup..."
/app/runner.sh bienici &
sleep 5
/app/runner.sh mubawab &
sleep 5
/app/runner.sh mktlist &
sleep 5
/app/runner.sh propertyfinder &
sleep 5
/app/runner.sh sync &

# ── Start cron in foreground (keeps container alive) ──
exec cron -f