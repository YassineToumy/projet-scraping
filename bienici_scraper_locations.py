#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bien'ici Scraper — Locations (Rentals) — Par département
Stratégie : résolution zoneIds via suggest API + scraping via realEstateAds.json

Usage:
    python bienici_scraper_locations.py
    python bienici_scraper_locations.py --stats
    python bienici_scraper_locations.py --dept 75
"""

import os
import json
import time
import random
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
from retry import retry
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIGURATION — all from .env
# ============================================================

MONGODB_URI = os.environ["MONGODB_URI"]
MONGODB_DATABASE = os.getenv("MONGO_BIENICI_DB", "bienici")
COLLECTION_NAME = os.getenv("MONGO_BIENICI_COL_RAW", "locations")
PROGRESS_COLLECTION = os.getenv("MONGO_BIENICI_COL_PROGRESS", "locations_scraping_progress")

BIENICI_SEARCH_URL = "https://www.bienici.com/realEstateAds.json"
BIENICI_SUGGEST_URL = "https://res.bienici.com/suggest.json"
RESULTS_PER_PAGE = 24
MAX_RESULTS = 2500

FILTER_TYPE = "rent"
PROPERTY_TYPES = ["flat", "house"]
MAX_RETRIES = 3
MIN_SUCCESS_RATIO = 0.1

PRICE_MIN = 0
PRICE_MAX = 15_000
MIN_PRICE_RANGE = 100

REQUEST_DELAY_MIN = 2
REQUEST_DELAY_MAX = 5
PAGE_DELAY_MIN = 3
PAGE_DELAY_MAX = 8
LONG_PAUSE_EVERY = random.randint(15, 25)
LONG_PAUSE_MIN = 15
LONG_PAUSE_MAX = 45
DEPT_PAUSE_MIN = 10
DEPT_PAUSE_MAX = 30

# ============================================================
# DÉPARTEMENTS FRANÇAIS
# ============================================================

DEPARTMENTS = [
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
    "11", "12", "13", "14", "15", "16", "17", "18", "19", "21",
    "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
    "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
    "90", "91", "92", "93", "94", "95",
    "971", "972", "973", "974", "976",
]

DEPARTMENT_NAMES = {
    "01": "ain", "02": "aisne", "03": "allier", "04": "alpes-de-haute-provence",
    "05": "hautes-alpes", "06": "alpes-maritimes", "07": "ardeche", "08": "ardennes",
    "09": "ariege", "10": "aube", "11": "aude", "12": "aveyron",
    "13": "bouches-du-rhone", "14": "calvados", "15": "cantal", "16": "charente",
    "17": "charente-maritime", "18": "cher", "19": "correze", "21": "cote-d-or",
    "22": "cotes-d-armor", "23": "creuse", "24": "dordogne", "25": "doubs",
    "26": "drome", "27": "eure", "28": "eure-et-loir", "29": "finistere",
    "2A": "corse-du-sud", "2B": "haute-corse",
    "30": "gard", "31": "haute-garonne", "32": "gers", "33": "gironde",
    "34": "herault", "35": "ille-et-vilaine", "36": "indre", "37": "indre-et-loire",
    "38": "isere", "39": "jura", "40": "landes", "41": "loir-et-cher",
    "42": "loire", "43": "haute-loire", "44": "loire-atlantique", "45": "loiret",
    "46": "lot", "47": "lot-et-garonne", "48": "lozere", "49": "maine-et-loire",
    "50": "manche", "51": "marne", "52": "haute-marne", "53": "mayenne",
    "54": "meurthe-et-moselle", "55": "meuse", "56": "morbihan", "57": "moselle",
    "58": "nievre", "59": "nord", "60": "oise", "61": "orne",
    "62": "pas-de-calais", "63": "puy-de-dome", "64": "pyrenees-atlantiques",
    "65": "hautes-pyrenees", "66": "pyrenees-orientales", "67": "bas-rhin",
    "68": "haut-rhin", "69": "rhone", "70": "haute-saone", "71": "saone-et-loire",
    "72": "sarthe", "73": "savoie", "74": "haute-savoie", "75": "paris",
    "76": "seine-maritime", "77": "seine-et-marne", "78": "yvelines",
    "79": "deux-sevres", "80": "somme", "81": "tarn", "82": "tarn-et-garonne",
    "83": "var", "84": "vaucluse", "85": "vendee", "86": "vienne",
    "87": "haute-vienne", "88": "vosges", "89": "yonne", "90": "territoire-de-belfort",
    "91": "essonne", "92": "hauts-de-seine", "93": "seine-saint-denis",
    "94": "val-de-marne", "95": "val-d-oise",
    "971": "guadeloupe", "972": "martinique", "973": "guyane",
    "974": "reunion", "976": "mayotte",
}

# ============================================================
# HTTP SESSION
# ============================================================

HEADERS = {
    "authority": "www.bienici.com",
    "accept": "*/*",
    "accept-language": "fr-FR,fr;q=0.9",
    "referer": "https://www.bienici.com/recherche/location/france/appartement",
    "sec-ch-ua": '"Chromium";v="131", "Google Chrome";v="131"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
}

SUGGEST_HEADERS = {
    "accept": "*/*",
    "accept-language": "fr-FR,fr;q=0.9",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

session = requests.Session()
session.headers.update(HEADERS)


# ============================================================
# ZONE ID RESOLUTION
# ============================================================

@retry(Exception, tries=3, delay=5, backoff=2)
def resolve_zone_id(dept_code: str) -> Optional[str]:
    query = DEPARTMENT_NAMES.get(dept_code, dept_code)
    url = f"{BIENICI_SUGGEST_URL}?q={query}"

    resp = requests.get(url, headers=SUGGEST_HEADERS)
    if resp.status_code != 200:
        print(f"  ⚠️  Suggest API {resp.status_code} pour {query}")
        return None

    results = resp.json()
    if not results:
        print(f"  ⚠️  Aucun résultat suggest pour {query}")
        return None

    for r in results:
        if r.get("type") == "department":
            ref = r.get("ref", "")
            if ref == dept_code or r.get("insee_code") == dept_code:
                zone_ids = r.get("zoneIds", [])
                if zone_ids:
                    print(f"  ✅ {dept_code} ({query}) → zoneId {zone_ids[0]}")
                    return zone_ids[0]

    for r in results:
        if r.get("type") == "department":
            zone_ids = r.get("zoneIds", [])
            if zone_ids:
                print(f"  ✅ {dept_code} ({query}) → zoneId {zone_ids[0]} (fallback)")
                return zone_ids[0]

    for r in results:
        zone_ids = r.get("zoneIds", [])
        if zone_ids:
            print(f"  ⚠️  {dept_code} ({query}) → zoneId {zone_ids[0]} (type={r.get('type')})")
            return zone_ids[0]

    print(f"  ❌ Pas de zoneId pour {dept_code}")
    return None


# ============================================================
# SEARCH API
# ============================================================

def build_filters(zone_id: str, price_min: int = None, price_max: int = None,
                  page: int = 1) -> str:
    filters = {
        "size": RESULTS_PER_PAGE,
        "from": (page - 1) * RESULTS_PER_PAGE,
        "filterType": FILTER_TYPE,
        "propertyType": PROPERTY_TYPES,
        "page": page,
        "resultsPerPage": RESULTS_PER_PAGE,
        "maxAuthorizedResults": MAX_RESULTS,
        "sortBy": "relevance",
        "sortOrder": "desc",
        "onTheMarket": [True],
        "zoneIdsByTypes": {"zoneIds": [zone_id]},
    }
    if price_min is not None:
        filters["minPrice"] = price_min
    if price_max is not None:
        filters["maxPrice"] = price_max
    return json.dumps(filters, separators=(",", ":"))


@retry(Exception, tries=MAX_RETRIES, delay=5, backoff=2)
def search_ads(zone_id: str, price_min: int = None, price_max: int = None,
               page: int = 1) -> Optional[dict]:
    filters_json = build_filters(zone_id, price_min, price_max, page)
    params = {"filters": filters_json}

    time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
    resp = session.get(BIENICI_SEARCH_URL, params=params)

    if resp.status_code == 403:
        print("    🛡️  403 Forbidden — pause longue...")
        time.sleep(random.uniform(60, 120))
        raise Exception("403 Forbidden")

    if resp.status_code != 200:
        print(f"    ⚠️  HTTP {resp.status_code}")
        raise Exception(f"HTTP {resp.status_code}")

    return resp.json()


def get_total(zone_id: str, price_min: int = None, price_max: int = None) -> int:
    result = search_ads(zone_id, price_min, price_max, page=1)
    if result:
        return result.get("total", 0)
    return 0


# ============================================================
# MONGODB
# ============================================================

class BieniciDB:
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[MONGODB_DATABASE]
        self.collection = self.db[COLLECTION_NAME]
        self.progress = self.db[PROGRESS_COLLECTION]
        self.stats = {
            "total_scraped": 0, "inserted": 0,
            "skipped": 0, "errors": 0,
        }
        self._seen_ids = set()
        self._load_existing_ids()
        self._setup_indexes()

    def _load_existing_ids(self):
        print("📦 Chargement des IDs existants...")
        for doc in self.collection.find({}, {"id": 1, "_id": 0}):
            aid = doc.get("id")
            if aid:
                self._seen_ids.add(aid)
        print(f"  ✅ {len(self._seen_ids)} annonces en base\n")

    def _setup_indexes(self):
        self.collection.create_index([("id", ASCENDING)], unique=True, name="id_unique")
        self.collection.create_index([("departmentCode", ASCENDING)])
        self.collection.create_index([("postalCode", ASCENDING)])
        self.collection.create_index([("city", ASCENDING)])
        self.collection.create_index([("price", ASCENDING)])
        self.collection.create_index([("propertyType", ASCENDING)])
        self.collection.create_index([("surfaceArea", ASCENDING)])
        self.progress.create_index(
            [("dept", ASCENDING)], unique=True, name="progress_unique"
        )

    def is_completed(self, dept: str) -> bool:
        return self.progress.find_one({"dept": dept, "status": "completed"}) is not None

    def mark_completed(self, dept: str, found: int, expected: int):
        if expected > 0 and found == 0:
            self.progress.update_one(
                {"dept": dept},
                {"$set": {"status": "incomplete", "found": found, "expected": expected,
                          "last_attempt": datetime.now(timezone.utc)}},
                upsert=True)
            return
        if expected > 0 and found < expected * MIN_SUCCESS_RATIO:
            self.progress.update_one(
                {"dept": dept},
                {"$set": {"status": "incomplete", "found": found, "expected": expected,
                          "last_attempt": datetime.now(timezone.utc)}},
                upsert=True)
            return
        self.progress.update_one(
            {"dept": dept},
            {"$set": {"status": "completed", "found": found, "expected": expected,
                      "completed_at": datetime.now(timezone.utc)}},
            upsert=True)

    def save_ads(self, ads: List[dict]) -> Tuple[int, int]:
        inserted = 0
        skipped = 0
        now = datetime.now(timezone.utc)

        for ad in ads:
            ad_id = ad.get("id")
            if not ad_id:
                skipped += 1
                continue
            if ad_id in self._seen_ids:
                skipped += 1
                self.stats["skipped"] += 1
                continue

            self._seen_ids.add(ad_id)
            ad["scraped_at"] = now
            ad["created_at"] = now

            try:
                self.collection.insert_one(ad)
                inserted += 1
                self.stats["inserted"] += 1
            except DuplicateKeyError:
                skipped += 1
                self.stats["skipped"] += 1
            except Exception as e:
                self.stats["errors"] += 1
                print(f"    ⚠️  DB: {str(e)[:80]}")

        self.stats["total_scraped"] += len(ads)
        return inserted, skipped

    def get_progress_summary(self) -> Dict:
        completed = self.progress.count_documents({"status": "completed"})
        incomplete = self.progress.count_documents({"status": "incomplete"})
        total = len(DEPARTMENTS)
        return {"completed": completed, "incomplete": incomplete,
                "total": total, "remaining": total - completed}

    def print_stats(self):
        total_db = self.collection.count_documents({})
        p = self.get_progress_summary()
        print(f"\n{'='*60}")
        print(f"📊 STATISTIQUES FINALES")
        print(f"{'='*60}")
        print(f"  Total scrapé:        {self.stats['total_scraped']}")
        print(f"  🆕 Nouvelles:         {self.stats['inserted']}")
        print(f"  ⭕ Doublons:          {self.stats['skipped']}")
        print(f"  ❌ Erreurs:            {self.stats['errors']}")
        print(f"  📦 Total en DB:        {total_db}")
        print(f"  🗂 Terminés:           {p['completed']}/{p['total']} depts")
        print(f"  ⚠️  Incomplets:        {p['incomplete']} depts")
        print(f"{'='*60}\n")

    def close(self):
        self.client.close()


# ============================================================
# PAGE COUNTER + RATE LIMITING
# ============================================================

class PageCounter:
    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1
        if self.count > 0 and self.count % LONG_PAUSE_EVERY == 0:
            pause = random.uniform(LONG_PAUSE_MIN, LONG_PAUSE_MAX)
            print(f"\n    ☕ Pause ({pause:.0f}s) — {self.count} pages...")
            time.sleep(pause)


# ============================================================
# SCRAPING
# ============================================================

def scrape_paginated(zone_id: str, db: BieniciDB, counter: PageCounter,
                     label: str, price_min: int = None,
                     price_max: int = None) -> int:
    total_found = 0
    consecutive_empty = 0
    consecutive_dupes = 0
    max_pages = MAX_RESULTS // RESULTS_PER_PAGE

    for page_num in range(1, max_pages + 1):
        try:
            result = search_ads(zone_id, price_min, price_max, page=page_num)
        except Exception:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
            continue

        if not result:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
            continue

        ads = result.get("realEstateAds", [])
        if not ads:
            break

        consecutive_empty = 0
        counter.increment()

        inserted, skipped = db.save_ads(ads)
        total_found += len(ads)

        print(f"    p.{page_num}: {len(ads)} ann. "
              f"(🆕 {inserted} ⭕ {skipped}) [{label}]")

        if inserted == 0 and skipped > 0:
            consecutive_dupes += 1
            if consecutive_dupes >= 5:
                print(f"    ⏩ 5 pages de doublons — skip")
                break
        else:
            consecutive_dupes = 0

        total_available = result.get("total", 0)
        if page_num * RESULTS_PER_PAGE >= total_available:
            break

        time.sleep(random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX))

    return total_found


def scrape_with_subdivision(zone_id: str, dept: str, db: BieniciDB,
                            counter: PageCounter, price_min: int = PRICE_MIN,
                            price_max: int = PRICE_MAX, depth: int = 0) -> Tuple[int, int]:
    indent = "  " * (depth + 1)
    label = f"d_{dept} {price_min}-{price_max}€"

    try:
        total = get_total(zone_id, price_min, price_max)
    except Exception:
        print(f"{indent}⚠️  {label} → erreur")
        return 0, -1

    if total == 0:
        print(f"{indent}ℹ️  {label} → 0 annonces")
        return 0, 0

    print(f"{indent}🔎 {label} → {total} annonces", end="")

    if total <= MAX_RESULTS:
        print(f" ✅ scraping direct")
        found = scrape_paginated(zone_id, db, counter, label, price_min, price_max)
        return found, total

    price_range = price_max - price_min
    if price_range <= MIN_PRICE_RANGE:
        print(f" ⚠️  fourchette min, scraping partiel")
        found = scrape_paginated(zone_id, db, counter, label, price_min, price_max)
        return found, total

    mid = price_min + price_range // 2
    print(f" → subdivision [{price_min}-{mid}] + [{mid}-{price_max}]")

    f1, e1 = scrape_with_subdivision(zone_id, dept, db, counter,
                                      price_min, mid, depth + 1)
    time.sleep(random.uniform(2, 5))
    f2, e2 = scrape_with_subdivision(zone_id, dept, db, counter,
                                      mid, price_max, depth + 1)

    found = f1 + f2
    if e1 == -1 or e2 == -1:
        return found, -1
    return found, total


def scrape_department(dept: str, zone_id: str, db: BieniciDB,
                      counter: PageCounter) -> int:
    if db.is_completed(dept):
        print(f"  ⭕ d_{dept} → déjà terminé")
        return 0

    print(f"\n  📦 Département {dept}")
    print(f"  {'─'*50}")

    found, expected = scrape_with_subdivision(zone_id, dept, db, counter)

    if expected == -1:
        print(f"  ⚠️  d_{dept}: erreur — sera re-tenté")
    else:
        db.mark_completed(dept, found, expected)

    return found


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Bien'ici Scraper — Locations")
    parser.add_argument("--stats", action="store_true", help="Stats seulement")
    parser.add_argument("--dept", type=str, help="Scraper un seul département")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("🏠 BIEN'ICI SCRAPER — LOCATIONS (RENTALS)")
    print(f"   Types: {', '.join(PROPERTY_TYPES)}")
    print(f"   Prix: {PRICE_MIN}-{PRICE_MAX}€/mois")
    print(f"   DB: {MONGODB_DATABASE}/{COLLECTION_NAME}")
    print(f"   Mode: requests HTTP (pas de browser)")
    print("=" * 60 + "\n")

    db = BieniciDB()

    if args.stats:
        db.print_stats()
        db.close()
        return

    if args.dept:
        depts = [args.dept]
    else:
        depts = DEPARTMENTS

    zone_mapping = {}
    print("🔍 Résolution des zoneIds par département...\n")
    for dept in depts:
        zid = resolve_zone_id(dept)
        if zid:
            zone_mapping[dept] = zid
        time.sleep(random.uniform(0.3, 0.8))

    if not zone_mapping:
        print("❌ Aucun zoneId résolu")
        db.close()
        return

    print(f"\n✅ {len(zone_mapping)}/{len(depts)} départements prêts\n")

    counter = PageCounter()
    p = db.get_progress_summary()
    if p["completed"] > 0:
        print(f"🔄 Reprise: {p['completed']}/{p['total']} terminés\n")

    try:
        for i, dept in enumerate(depts):
            if dept not in zone_mapping:
                continue

            zone_id = zone_mapping[dept]
            p = db.get_progress_summary()

            print(f"\n{'='*60}")
            print(f"🗂 DEPT {dept} ({i+1}/{len(depts)}) — "
                  f"Progression: {p['completed']}/{p['total']}")
            print(f"{'='*60}")

            dept_total = scrape_department(dept, zone_id, db, counter)

            print(f"\n📦 d_{dept}: {dept_total} annonces | "
                  f"DB: {db.stats['inserted']} nouvelles")

            if i < len(depts) - 1:
                pause = random.uniform(DEPT_PAUSE_MIN, DEPT_PAUSE_MAX)
                print(f"   ⏳ Pause ({pause:.0f}s)...")
                time.sleep(pause)

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrompu — progression sauvegardée !")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.print_stats()
        db.close()

    print("✅ Terminé!\n")


if __name__ == "__main__":
    main()