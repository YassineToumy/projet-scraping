#!/usr/bin/env python3
"""
Scraper Health Monitor — checks MongoDB counts, alerts if no new ads in 24h.
Config via .env
"""

import os
import requests
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.environ["MONGODB_URI"]
ALERT_WEBHOOK = os.getenv("ALERT_WEBHOOK", "")

SCRAPERS = {
    "bienici": {
        "db": os.getenv("MONGO_BIENICI_DB", "bienici"),
        "collection": os.getenv("MONGO_BIENICI_COL_RAW", "locations"),
        "label": "Bien'ici (FR)",
    },
    "mubawab": {
        "db": os.getenv("MONGO_MUBAWAB_DB", "mubawab"),
        "collection": os.getenv("MONGO_MUBAWAB_COL", "locations"),
        "label": "Mubawab (TN)",
    },
    "mktlist": {
        "db": os.getenv("MONGO_MKTLIST_DB", "mktlist"),
        "collection": os.getenv("MONGO_MKTLIST_COL", "locations"),
        "label": "MktList (CA)",
    },
    "propertyfinder": {
        "db": os.getenv("MONGO_PROPERTYFINDER_DB", "propertyfinder"),
        "collection": os.getenv("MONGO_PROPERTYFINDER_COL", "locations"),
        "label": "PropertyFinder (EG)",
    },
}


def check_health():
    client = MongoClient(MONGODB_URI)
    now = datetime.now(timezone.utc)
    report = []
    alerts = []

    for name, cfg in SCRAPERS.items():
        col = client[cfg["db"]][cfg["collection"]]
        total = col.count_documents({})

        recent_filter = {
            "$or": [
                {"scraped_at": {"$gte": now - timedelta(hours=26)}},
                {"created_at": {"$gte": now - timedelta(hours=26)}},
                {"updated_at": {"$gte": now - timedelta(hours=26)}},
            ]
        }
        recent = col.count_documents(recent_filter)

        status = "✅" if recent > 0 else "⚠️"
        line = f"{status} {cfg['label']}: {total} total, {recent} last 24h"
        report.append(line)

        if recent == 0 and total > 0:
            alerts.append(f"{cfg['label']}: No new ads in 24h (total: {total})")

    client.close()

    print(f"\n{'='*50}")
    print(f"📊 Scraper Health — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*50}")
    for line in report:
        print(f"  {line}")
    print(f"{'='*50}\n")

    if alerts and ALERT_WEBHOOK:
        msg = "🚨 **Scraper Alert**\n" + "\n".join(f"- {a}" for a in alerts)
        try:
            requests.post(ALERT_WEBHOOK, json={"content": msg, "text": msg}, timeout=10)
            print("📤 Alert sent!")
        except Exception as e:
            print(f"Failed to send alert: {e}")

    return len(alerts) == 0


if __name__ == "__main__":
    healthy = check_health()
    exit(0 if healthy else 1)