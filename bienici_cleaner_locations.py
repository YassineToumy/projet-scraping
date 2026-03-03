#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bien'ici Data Cleaner v2 — Locations
Normalise les données brutes MongoDB → MongoDB clean collection
Prêt pour export vers PostgreSQL unifié multi-pays.

Usage:
    python bienici_cleaner_locations_v2.py
    python bienici_cleaner_locations_v2.py --dry-run    # Preview sans écriture
    python bienici_cleaner_locations_v2.py --sample 5   # Afficher N exemples
"""

import os
import re
import html
import argparse
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://root:root@187.77.168.42:27018")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "bienici")
SOURCE_COLLECTION = "locations"
CLEAN_COLLECTION = "locations_clean"
BATCH_SIZE = 500

# Validation thresholds (monthly rent in EUR)
MIN_PRICE = 50
MAX_PRICE = 20_000
MIN_SURFACE = 5
MAX_SURFACE = 500
MAX_ROOMS = 20
MIN_PRICE_PER_M2 = 2
MAX_PRICE_PER_M2 = 200

# ============================================================
# CLEANING HELPERS
# ============================================================

# Regex to strip HTML tags
RE_HTML = re.compile(r"<[^>]+>")
# Regex to strip agency fee boilerplate at end of description
RE_BOILERPLATE = re.compile(
    r"\s*(?:Loyer de|Soit avec Assurance|Les honoraires|Vous pouvez consulter|"
    r"Montant estimé des dépenses|Prix moyens des énergies|"
    r"Les informations sur les risques|Service facultatif|"
    r"Contribution annuelle).*",
    re.DOTALL | re.IGNORECASE,
)

ENERGY_MAP = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7}


def clean_description(raw: str) -> str | None:
    """Strip HTML tags, HTML entities, agency boilerplate, normalize whitespace."""
    if not raw:
        return None
    text = RE_HTML.sub("", raw)       # Remove HTML tags: <b>, <br/>, <p>, etc.
    text = html.unescape(text)        # Decode HTML entities: &amp; &nbsp; &lt; etc.
    text = RE_BOILERPLATE.sub("", text)
    # Normalize whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    return text if len(text) > 20 else None


def extract_photo_urls(photos: list) -> list[str]:
    """Extract unique clean CDN URLs from photos array."""
    if not photos or not isinstance(photos, list):
        return []
    seen = set()
    urls = []
    for p in photos:
        url = None
        if isinstance(p, dict):
            # Prefer the CDN url (file.bienici.com)
            url = p.get("url") or p.get("url_photo")
        elif isinstance(p, str):
            url = p
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def extract_coordinates(doc: dict) -> tuple[float | None, float | None]:
    """Extract lat/lon from blurInfo (position > centroid)."""
    blur = doc.get("blurInfo")
    if not blur or not isinstance(blur, dict):
        return None, None
    pos = blur.get("position") or blur.get("centroid") or {}
    lat = pos.get("lat")
    lon = pos.get("lon")
    if lat is not None and lon is not None:
        # Basic sanity check for France + DOM-TOM
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return round(lat, 6), round(lon, 6)
    return None, None


def extract_virtual_tour_urls(tours: list) -> list[str]:
    """Keep only the URLs from virtual tours."""
    if not tours or not isinstance(tours, list):
        return []
    urls = []
    for t in tours:
        if isinstance(t, dict):
            url = t.get("url")
            if url:
                urls.append(url)
    return urls


def flatten_district(district: dict) -> dict | None:
    """Keep only useful district fields."""
    if not district or not isinstance(district, dict):
        return None
    flat = {}
    for key in ("name", "libelle", "cp", "code_insee", "insee_code", "postal_code"):
        val = district.get(key)
        if val is not None:
            flat[key] = val
    return flat if flat else None


def derive_department_code(doc: dict) -> str | None:
    """Get department code from field or postal code."""
    dept = doc.get("departmentCode")
    if dept:
        return str(dept)
    pc = doc.get("postalCode")
    if pc:
        pc_str = str(pc)
        # DOM-TOM: 971xx, 972xx, etc.
        if len(pc_str) == 5 and pc_str[:3] in ("971", "972", "973", "974", "976"):
            return pc_str[:3]
        if len(pc_str) >= 2:
            return pc_str[:2]
    return None


# ============================================================
# MAIN CLEAN FUNCTION
# ============================================================

def clean_document(doc: dict) -> dict:
    """Full cleaning pipeline for one Bien'ici document."""
    c = {}

    # === IDENTIFICATION ===
    c["source_id"] = doc.get("id")
    c["source"] = "bienici"
    c["country"] = "FR"
    c["reference"] = doc.get("reference")
    c["transaction_type"] = "rent"  # All our data is rentals

    # === PROPERTY TYPE ===
    prop_type = doc.get("propertyType")
    # Normalize to our unified types
    type_map = {"flat": "apartment", "house": "house", "parking": "parking",
                "loft": "loft", "castle": "house", "townhouse": "house"}
    c["property_type"] = type_map.get(prop_type, prop_type)

    # === LOCATION ===
    c["city"] = doc.get("city")
    c["postal_code"] = doc.get("postalCode")
    c["department_code"] = derive_department_code(doc)
    c["address_known"] = doc.get("addressKnown")

    district = flatten_district(doc.get("district"))
    if district:
        c["district_name"] = district.get("libelle") or district.get("name")
        c["insee_code"] = district.get("insee_code") or district.get("code_insee")

    lat, lon = extract_coordinates(doc)
    if lat is not None:
        c["latitude"] = lat
        c["longitude"] = lon

    # === PROPERTY DETAILS ===
    c["surface_m2"] = doc.get("surfaceArea")
    c["floor"] = doc.get("floor")
    c["rooms"] = doc.get("roomsQuantity")
    c["bedrooms"] = doc.get("bedroomsQuantity")
    c["bathrooms"] = doc.get("bathroomsQuantity")
    c["shower_rooms"] = doc.get("showerRoomsQuantity")
    c["terraces"] = doc.get("terracesQuantity")
    c["balconies"] = doc.get("balconyQuantity")
    c["parking_spots"] = doc.get("parkingPlacesQuantity")
    c["cellars"] = doc.get("cellarsOrUndergroundsQuantity")
    c["is_new"] = doc.get("newProperty")
    c["is_furnished"] = doc.get("isFurnished")
    c["is_accessible"] = doc.get("isDisabledPeopleFriendly")
    c["has_elevator"] = doc.get("hasElevator")
    c["heating"] = doc.get("heating")
    c["optical_fiber"] = doc.get("opticalFiberStatus")

    # === PRICE & CHARGES ===
    c["price"] = doc.get("price")
    c["currency"] = "EUR"
    c["charges"] = doc.get("charges")
    c["agency_fee"] = doc.get("agencyRentalFee")
    c["price_decreased"] = doc.get("priceHasDecreased")

    # === ENERGY ===
    c["energy_class"] = doc.get("energyClassification")
    c["ghg_class"] = doc.get("greenhouseGazClassification")
    c["energy_value"] = doc.get("energyValue")
    c["ghg_value"] = doc.get("greenhouseGazValue")
    c["energy_diag_date"] = doc.get("energyPerformanceDiagnosticDate")
    c["min_energy_cost"] = doc.get("minEnergyConsumption")
    c["max_energy_cost"] = doc.get("maxEnergyConsumption")

    # === DESCRIPTION (cleaned) ===
    c["description"] = clean_description(doc.get("description"))

    # === PHOTOS (deduplicated CDN URLs only) ===
    photo_urls = extract_photo_urls(doc.get("photos"))
    c["photos"] = photo_urls if photo_urls else []
    c["photos_count"] = len(photo_urls)

    # === VIRTUAL TOURS ===
    tour_urls = extract_virtual_tour_urls(doc.get("virtualTours"))
    if tour_urls:
        c["virtual_tours"] = tour_urls

    # === AGENCY / SOURCE ===
    c["posted_by_pro"] = doc.get("adCreatedByPro")
    c["agency_name"] = doc.get("accountDisplayName")
    c["agency_id"] = doc.get("customerId")
    c["is_exclusive"] = doc.get("isBienIciExclusive")

    # === DERIVED FEATURES ===
    surface = c.get("surface_m2")
    price = c.get("price")
    rooms = c.get("rooms")
    bedrooms = c.get("bedrooms")

    if surface and price and surface > 0:
        c["price_per_m2"] = round(price / surface, 2)

    if surface and rooms and rooms > 0:
        c["surface_per_room"] = round(surface / rooms, 2)

    if surface and bedrooms and bedrooms > 0:
        c["surface_per_bedroom"] = round(surface / bedrooms, 2)

    if price and c.get("charges"):
        c["rent_excluding_charges"] = round(price - c["charges"], 2)

    # Equipment score
    equip_bool = ["has_elevator", "is_furnished", "is_accessible"]
    equip_qty = ["terraces", "balconies", "parking_spots", "cellars"]
    score = sum(1 for f in equip_bool if c.get(f) is True)
    score += sum(1 for f in equip_qty if (c.get(f) or 0) > 0)
    c["equipment_score"] = score

    # Energy as numeric
    if c.get("energy_class"):
        c["energy_numeric"] = ENERGY_MAP.get(c["energy_class"].upper())
    if c.get("ghg_class"):
        c["ghg_numeric"] = ENERGY_MAP.get(c["ghg_class"].upper())

    # === METADATA ===
    c["cleaned_at"] = datetime.now(timezone.utc)

    # Remove None values
    c = {k: v for k, v in c.items() if v is not None}

    return c


# ============================================================
# VALIDATION
# ============================================================

def validate(doc: dict) -> tuple[bool, str | None]:
    """Validate a cleaned document."""
    price = doc.get("price")
    if not price or price < MIN_PRICE or price > MAX_PRICE:
        return False, "invalid_price"

    surface = doc.get("surface_m2")
    if not surface or surface < MIN_SURFACE or surface > MAX_SURFACE:
        return False, "invalid_surface"

    prop = doc.get("property_type")
    if prop not in ("apartment", "house"):
        return False, "invalid_type"

    if not doc.get("city") or not doc.get("postal_code"):
        return False, "missing_location"

    rooms = doc.get("rooms")
    if rooms and rooms > MAX_ROOMS:
        return False, "aberrant_rooms"

    ppm = doc.get("price_per_m2")
    if ppm and (ppm < MIN_PRICE_PER_M2 or ppm > MAX_PRICE_PER_M2):
        return False, "aberrant_price_m2"

    return True, None


# ============================================================
# PIPELINE
# ============================================================

def connect_db():
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DATABASE]
    return client, db


def setup_clean_collection(db):
    col = db[CLEAN_COLLECTION]
    col.drop()
    print(f"🗑️  '{CLEAN_COLLECTION}' reset")

    col.create_index([("source_id", ASCENDING)], unique=True, name="source_id_unique")
    col.create_index([("city", ASCENDING)])
    col.create_index([("postal_code", ASCENDING)])
    col.create_index([("department_code", ASCENDING)])
    col.create_index([("property_type", ASCENDING)])
    col.create_index([("price", ASCENDING)])
    col.create_index([("surface_m2", ASCENDING)])
    col.create_index([("is_furnished", ASCENDING)])
    col.create_index([("country", ASCENDING)])
    col.create_index([
        ("city", ASCENDING),
        ("property_type", ASCENDING),
        ("price", ASCENDING),
    ])
    print("✅ Indexes created\n")
    return col


def insert_batch(col, batch):
    ins, dup = 0, 0
    try:
        r = col.insert_many(batch, ordered=False)
        ins = len(r.inserted_ids)
    except BulkWriteError as e:
        ins = e.details.get("nInserted", 0)
        dup = len(batch) - ins
    return ins, dup


def run(source, clean, dry_run=False):
    total = source.count_documents({})
    print(f"📊 Source documents: {total}\n")
    if total == 0:
        print("⚠️  Nothing to process.")
        return

    stats = {
        "total": total, "cleaned": 0, "inserted": 0,
        "invalid_price": 0, "invalid_surface": 0, "invalid_type": 0,
        "missing_location": 0, "aberrant_rooms": 0, "aberrant_price_m2": 0,
        "duplicates": 0, "errors": 0,
    }

    batch = []
    for i, doc in enumerate(source.find({}, batch_size=BATCH_SIZE)):
        try:
            cleaned = clean_document(doc)
            stats["cleaned"] += 1

            valid, reason = validate(cleaned)
            if not valid:
                stats[reason] = stats.get(reason, 0) + 1
                continue

            cleaned.pop("_id", None)

            if dry_run:
                stats["inserted"] += 1
                continue

            batch.append(cleaned)

            if len(batch) >= BATCH_SIZE:
                ins, dup = insert_batch(clean, batch)
                stats["inserted"] += ins
                stats["duplicates"] += dup
                batch = []
                pct = (i + 1) / total * 100
                print(f"   ⏳ {i+1}/{total} ({pct:.1f}%) — ✅ {stats['inserted']}",
                      end="\r", flush=True)

        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                print(f"\n   ⚠️  Error on {doc.get('id')}: {str(e)[:100]}")

    if batch and not dry_run:
        ins, dup = insert_batch(clean, batch)
        stats["inserted"] += ins
        stats["duplicates"] += dup

    print_stats(stats, dry_run)


def print_stats(s, dry_run=False):
    total = s["total"]
    ins = s["inserted"]
    rejected = s["cleaned"] - ins - s["duplicates"]

    print(f"\n\n{'='*60}")
    print(f"📊 CLEANING RESULTS {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*60}")
    print(f"   📥 Total:        {total}")
    print(f"   🧹 Cleaned:      {s['cleaned']}")
    print(f"   ✅ Valid:         {ins} ({ins/max(total,1)*100:.1f}%)")
    print(f"   ❌ Rejected:      {rejected}")
    if rejected > 0:
        print(f"      💰 Bad price:       {s.get('invalid_price',0)}")
        print(f"      📐 Bad surface:     {s.get('invalid_surface',0)}")
        print(f"      🏠 Bad type:        {s.get('invalid_type',0)}")
        print(f"      📍 No location:     {s.get('missing_location',0)}")
        print(f"      🔢 Bad rooms:       {s.get('aberrant_rooms',0)}")
        print(f"      💵 Bad price/m²:    {s.get('aberrant_price_m2',0)}")
    if s["duplicates"]:
        print(f"   🔁 Duplicates:   {s['duplicates']}")
    if s["errors"]:
        print(f"   ⚠️  Errors:       {s['errors']}")
    print(f"{'='*60}")


def show_sample(clean, n=3):
    """Print sample cleaned documents."""
    print(f"\n📄 SAMPLE CLEANED DOCUMENTS ({n}):")
    for doc in clean.find({}, {"_id": 0}).limit(n):
        print("─" * 60)
        for k, v in doc.items():
            if k == "photos":
                print(f"   {k}: [{len(v)} urls]")
            elif k == "description":
                print(f"   {k}: {str(v)[:80]}...")
            elif isinstance(v, dict):
                print(f"   {k}: {v}")
            else:
                print(f"   {k}: {v}")
    print("─" * 60)


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Bien'ici Cleaner v2")
    parser.add_argument("--dry-run", action="store_true", help="Validate without writing")
    parser.add_argument("--sample", type=int, default=0, help="Show N sample docs after")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("🧹 BIEN'ICI CLEANER v2 — LOCATIONS")
    print(f"   {SOURCE_COLLECTION} → {CLEAN_COLLECTION}")
    print(f"   Mode: {'DRY RUN' if args.dry_run else 'WRITE'}")
    print("=" * 60 + "\n")

    client, db = connect_db()
    source = db[SOURCE_COLLECTION]

    if args.dry_run:
        run(source, None, dry_run=True)
    else:
        clean = setup_clean_collection(db)
        run(source, clean)
        if args.sample > 0:
            show_sample(clean, args.sample)
        print(f"\n✅ Done! {CLEAN_COLLECTION}: {clean.count_documents({})} docs")

    client.close()


if __name__ == "__main__":
    main()