#!/usr/bin/env python3
"""
MongoDB -> PostgreSQL Sync + Archive Checker
Runs continuously: syncs new listings, archives dead ones.

Usage:
    python sync_mongo_to_postgres.py              # Full cycle (sync + archive)
    python sync_mongo_to_postgres.py --sync-only  # Only sync new data
    python sync_mongo_to_postgres.py --archive-only # Only check dead listings
    python sync_mongo_to_postgres.py --once       # Run once and exit
"""

import os
import re
import json
import time
import logging
import argparse
from datetime import datetime, timezone

import requests
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG (all from .env)
# ============================================================

MONGO_URI = os.environ["MONGODB_URI"]
PG_DSN = os.environ["POSTGRES_DSN"]

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
ARCHIVE_REQUEST_DELAY = 2
ARCHIVE_REQUEST_TIMEOUT = 15
CYCLE_SLEEP_SECONDS = int(os.getenv("CYCLE_SLEEP", "86400"))

CHECK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

# Source configs — FIX: bienici reads from raw "locations" (not locations_clean)
# If you add a cleaning pipeline later, change MONGO_BIENICI_COL_SYNC to locations_clean
SOURCES = {
    "bienici": {
        "mongo_db": os.getenv("MONGO_BIENICI_DB", "bienici"),
        "mongo_col": os.getenv("MONGO_BIENICI_COL_RAW", "locations"),
        "pg_table": os.getenv("PG_TABLE_BIENICI", "bienici_listings"),
        "pg_archive": os.getenv("PG_ARCHIVE_BIENICI", "bienici_archive"),
        "pg_unique": "source_id",
        "mongo_unique": "id",
        "url_field": "url",
        "flag": "FR",
    },
    "mubawab": {
        "mongo_db": os.getenv("MONGO_MUBAWAB_DB", "mubawab"),
        "mongo_col": os.getenv("MONGO_MUBAWAB_COL", "locations"),
        "pg_table": os.getenv("PG_TABLE_MUBAWAB", "mubawab_listings"),
        "pg_archive": os.getenv("PG_ARCHIVE_MUBAWAB", "mubawab_archive"),
        "pg_unique": "ad_id",
        "mongo_unique": "ad_id",
        "url_field": "url",
        "flag": "TN",
    },
    "propertyfinder": {
        "mongo_db": os.getenv("MONGO_PROPERTYFINDER_DB", "propertyfinder"),
        "mongo_col": os.getenv("MONGO_PROPERTYFINDER_COL", "locations"),
        "pg_table": os.getenv("PG_TABLE_PROPERTYFINDER", "propertyfinder_listings"),
        "pg_archive": os.getenv("PG_ARCHIVE_PROPERTYFINDER", "propertyfinder_archive"),
        "pg_unique": "property_id",
        "mongo_unique": "property_id",
        "url_field": "url",
        "flag": "EG",
    },
    "mktlist": {
        "mongo_db": os.getenv("MONGO_MKTLIST_DB", "mktlist"),
        "mongo_col": os.getenv("MONGO_MKTLIST_COL", "locations"),
        "pg_table": os.getenv("PG_TABLE_MKTLIST", "mktlist_listings"),
        "pg_archive": os.getenv("PG_ARCHIVE_MKTLIST", "mktlist_archive"),
        "pg_unique": "url",
        "mongo_unique": "url",
        "url_field": "url",
        "flag": "CA",
    },
}

# Homepages for redirect detection
HOMEPAGE_REDIRECTS = {
    "https://www.bienici.com", "https://www.bienici.com/fr",
    "https://www.mubawab.tn", "https://www.mubawab.tn/fr",
    "https://www.propertyfinder.eg", "https://www.propertyfinder.eg/en",
    "https://www.mktlist.ca",
}

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sync")

# ============================================================
# HELPERS
# ============================================================

def parse_price_string(s):
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    cleaned = re.sub(r"[^\d.]", "", str(s).replace(",", "").replace("\xa0", ""))
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def parse_int_safe(v):
    if v is None:
        return None
    if isinstance(v, int):
        return v
    try:
        return int(str(v).strip())
    except (ValueError, TypeError):
        return None


def to_pg_timestamp(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, dict) and "$date" in v:
        try:
            return datetime.fromisoformat(v["$date"].replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    if isinstance(v, (int, float)):
        try:
            # Epoch seconds — mktlist uses time.time()
            if v > 1e12:
                v = v / 1000  # milliseconds -> seconds
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except (OSError, ValueError, OverflowError):
            return None
    return None


def to_pg_array(lst):
    if not lst or not isinstance(lst, list):
        return None
    cleaned = [str(x) for x in lst if x]
    return cleaned if cleaned else None


def parse_sqft_range(raw):
    if not raw:
        return None, None, None
    m = re.findall(r"(\d[\d,]*)", str(raw))
    nums = []
    for x in m:
        try:
            nums.append(int(x.replace(",", "")))
        except ValueError:
            pass
    if not nums:
        return None, None, None
    lo = nums[0]
    hi = nums[-1] if len(nums) > 1 else lo
    avg_m2 = int(round((lo + hi) / 2 * 0.092903))
    return lo, hi, avg_m2


def extract_city_province(title, address):
    city, province = None, None
    if title:
        parts = [p.strip() for p in title.split(",")]
        if len(parts) >= 3:
            province = parts[-2].strip() if parts[-2].strip() != "CA" else None
            city_part = parts[-3] if len(parts) >= 4 else parts[-2]
            city = re.sub(r"\s*\(.*?\)", "", city_part).strip()
    if not city and address:
        parts = [p.strip() for p in address.split(",")]
        for p in parts:
            clean = re.sub(r"\s*\(.*?\)", "", p).strip()
            if clean and not clean.startswith("#") and not re.match(r"^\d", clean):
                city = clean
                break
    return city, province


PF_KNOWN_AMENITIES = {
    "Furnished", "Built in Wardrobes", "Central A/C", "Covered Parking",
    "Kitchen Appliances", "Private Garden", "Study", "Shared Spa", "Security",
    "Swimming Pool", "Gym", "Elevator", "Maid's Room", "Maids Room",
    "Storage Room", "Pets Allowed", "Concierge", "Children's Play Area",
    "BBQ Area", "Jacuzzi", "Sauna", "Steam Room", "View of Landmark",
    "View of Water", "Shared Pool", "Private Pool", "Internet", "Balcony",
    "Walk-in Closet", "Built in Kitchen Appliances",
}


def clean_pf_amenities(raw):
    if not raw or not isinstance(raw, list):
        return None
    cleaned = [a for a in raw if a in PF_KNOWN_AMENITIES]
    return cleaned if cleaned else None


def clean_pf_description(raw):
    if not raw:
        return None
    if "BuyRentNew projects" in raw or "Log inApartments" in raw:
        m = re.search(r"sqm(.+?)(?:See full description|Property details)", raw, re.DOTALL)
        if m and len(m.group(1).strip()) > 20:
            return m.group(1).strip()
        m = re.search(
            r"((?:Apartment|Villa|Flat|Furnished|Brand new|Luxury|Spacious).*?)"
            r"(?:See full description|Property details|Property Type)",
            raw, re.DOTALL | re.IGNORECASE,
        )
        if m:
            return m.group(1).strip()
        return None
    return raw


def extract_mubawab_features(mf):
    if not mf or not isinstance(mf, dict):
        return {}
    return {
        "property_condition": mf.get("Etat"),
        "property_age": mf.get("Annees") or mf.get("Années"),
        "floor_number": mf.get("Étage du bien"),
        "orientation": mf.get("Orientation"),
        "floor_type": mf.get("Type du sol"),
    }


# ============================================================
# BIENICI ROW BUILDER
# Reads raw MongoDB docs (from scraper) and maps to PG columns
# ============================================================

def build_bienici_row(d):
    """Build PG row from raw bienici MongoDB document."""
    # The scraper stores the full API response — field names come from bienici API
    source_id = d.get("id")  # bienici uses "id" as unique key
    price = parse_price_string(d.get("price"))
    surface = d.get("surfaceArea")
    rooms = parse_int_safe(d.get("roomsQuantity"))
    bedrooms = parse_int_safe(d.get("bedroomsQuantity"))
    bathrooms = parse_int_safe(d.get("bathroomsQuantity"))
    shower_rooms = parse_int_safe(d.get("showerRoomsQuantity")) or 0
    floor = parse_int_safe(d.get("floor"))

    # Build URL if not present
    url = d.get("url")
    if not url and source_id:
        url = f"https://www.bienici.com/annonce/location/{source_id}"

    # Photos
    photos = []
    for p in (d.get("photos") or []):
        if isinstance(p, dict):
            photos.append(p.get("url") or p.get("url_photo") or "")
        elif isinstance(p, str):
            photos.append(p)
    photos = to_pg_array(photos)

    # Energy
    energy_class = d.get("energyClassification") or d.get("energyValue")
    ghg_class = d.get("greenhouseGasClassification") or d.get("greenhouseGasValue")
    energy_value = parse_int_safe(d.get("energyValue"))
    ghg_value = parse_int_safe(d.get("greenhouseGasValue"))

    # Price per m2
    price_per_m2 = None
    if price and surface and surface > 0:
        price_per_m2 = round(price / surface, 2)

    return (
        str(source_id),                                      # source_id
        d.get("reference"),                                  # reference
        url,                                                 # url
        d.get("city"),                                       # city
        d.get("postalCode"),                                 # postal_code
        d.get("departmentCode"),                             # department_code
        d.get("districtName"),                               # district_name
        d.get("inseeCode"),                                  # insee_code
        d.get("addressKnown", False),                        # address_known
        d.get("blurInfo", {}).get("position", {}).get("lat") or d.get("latitude"),
        d.get("blurInfo", {}).get("position", {}).get("lng") or d.get("longitude"),
        d.get("propertyType"),                               # property_type
        surface,                                             # surface_m2
        floor,                                               # floor
        rooms,                                               # rooms
        bedrooms,                                            # bedrooms
        bathrooms,                                           # bathrooms
        shower_rooms,                                        # shower_rooms
        parse_int_safe(d.get("terracesQuantity")) or 0,     # terraces
        parse_int_safe(d.get("balconyQuantity")) or 0,      # balconies
        parse_int_safe(d.get("parkingPlacesQuantity")) or 0, # parking_spots
        parse_int_safe(d.get("cellarsQuantity")) or 0,      # cellars
        d.get("newProperty", False),                         # is_new
        d.get("isFurnished", False),                         # is_furnished
        d.get("isDisabledPeopleFriendly", False),           # is_accessible
        d.get("hasElevator", False),                         # has_elevator
        d.get("heatingType"),                                # heating
        d.get("opticalFiberStatus"),                         # optical_fiber
        price,                                               # price
        "EUR",                                               # currency
        parse_price_string(d.get("charges")),               # charges
        parse_price_string(d.get("agencyFeeUrl") or d.get("feePercentage")),
        d.get("priceHasDecreased", False),                  # price_decreased
        parse_price_string(d.get("priceExcludingCharges")), # rent_excluding_charges
        price_per_m2,                                        # price_per_m2
        energy_class if isinstance(energy_class, str) else None,
        ghg_class if isinstance(ghg_class, str) else None,
        energy_value,                                        # energy_value
        ghg_value,                                           # ghg_value
        d.get("energyDiagnosticDate"),                      # energy_diag_date
        parse_price_string(d.get("minEnergyConsumptionCost")),
        parse_price_string(d.get("maxEnergyConsumptionCost")),
        energy_value,                                        # energy_numeric
        ghg_value,                                           # ghg_numeric
        d.get("description"),                                # description
        photos,                                              # photos
        len(photos) if photos else 0,                        # photos_count
        to_pg_array(d.get("virtualTours")),                 # virtual_tours
        not d.get("isPrivateSeller", False),                 # posted_by_pro
        d.get("agencyName") or (d.get("agency") or {}).get("name"),
        d.get("agencyId") or (d.get("agency") or {}).get("id"),
        d.get("isExclusiveSaleMandate", False),             # is_exclusive
        round(surface / rooms, 1) if surface and rooms and rooms > 0 else None,
        round(surface / bedrooms, 1) if surface and bedrooms and bedrooms > 0 else None,
        0,                                                   # equipment_score
        to_pg_timestamp(d.get("scraped_at")),               # cleaned_at
    )


BIENICI_INSERT_SQL = """
    INSERT INTO bienici_listings (
        source_id, reference, url, city, postal_code, department_code,
        district_name, insee_code, address_known, latitude, longitude,
        property_type, surface_m2, floor, rooms, bedrooms,
        bathrooms, shower_rooms, terraces, balconies, parking_spots,
        cellars, is_new, is_furnished, is_accessible, has_elevator,
        heating, optical_fiber, price, currency, charges,
        agency_fee, price_decreased, rent_excluding_charges, price_per_m2,
        energy_class, ghg_class, energy_value, ghg_value, energy_diag_date,
        min_energy_cost, max_energy_cost, energy_numeric, ghg_numeric,
        description, photos, photos_count, virtual_tours,
        posted_by_pro, agency_name, agency_id, is_exclusive,
        surface_per_room, surface_per_bedroom, equipment_score, cleaned_at
    ) VALUES %s
    ON CONFLICT (source_id) DO NOTHING
"""


def build_mubawab_row(d):
    mf = extract_mubawab_features(d.get("main_features"))
    images = to_pg_array(d.get("images"))
    return (
        str(d.get("ad_id")), d.get("url"), d.get("city"),
        d.get("country", "TN"), d.get("location_text"),
        d.get("latitude"), d.get("longitude"), d.get("property_type"),
        d.get("area_m2"), parse_int_safe(d.get("rooms")),
        parse_int_safe(d.get("bedrooms")), parse_int_safe(d.get("bathrooms")),
        d.get("title"), parse_price_string(d.get("price")),
        d.get("currency", "TND"), d.get("description"),
        to_pg_array(d.get("features")), mf.get("property_condition"),
        mf.get("property_age"), mf.get("floor_number"),
        mf.get("orientation"), mf.get("floor_type"),
        images, len(images) if images else 0,
        d.get("seller_name"), d.get("seller_type"),
        to_pg_timestamp(d.get("scraped_at")),
    )


MUBAWAB_INSERT_SQL = """
    INSERT INTO mubawab_listings (
        ad_id, url, city, country, location_text,
        latitude, longitude, property_type, area_m2, rooms,
        bedrooms, bathrooms, title, price, currency,
        description, features, property_condition, property_age,
        floor_number, orientation, floor_type,
        images, images_count, seller_name, seller_type, scraped_at
    ) VALUES %s
    ON CONFLICT (ad_id) DO NOTHING
"""


def build_propertyfinder_row(d):
    amenities = clean_pf_amenities(d.get("amenities"))
    description = clean_pf_description(d.get("description"))
    ps = d.get("property_size") or {}
    pi = d.get("price_insights") or {}
    furn = d.get("furnished")
    is_furnished = True if furn == "furnished" else (False if furn else None)
    images = to_pg_array(d.get("images"))
    return (
        d.get("property_id"), d.get("reference"), d.get("url"),
        d.get("city"), d.get("district"), d.get("compound"),
        d.get("location_full"), d.get("property_type"),
        parse_int_safe(ps.get("sqm")), parse_int_safe(ps.get("sqft")),
        parse_int_safe(d.get("bedrooms")), parse_int_safe(d.get("bathrooms")),
        is_furnished, d.get("available_from"),
        d.get("title"), parse_price_string(d.get("price_value")),
        d.get("price_raw"), d.get("currency", "EGP"),
        d.get("price_period", "monthly"),
        description, amenities, images,
        len(images) if images else 0,
        parse_price_string(pi.get("avg_rent")),
        parse_int_safe(pi.get("avg_size")),
        parse_int_safe(pi.get("vs_avg_pct")), pi.get("vs_avg_dir"),
        d.get("agent_name"), d.get("agency_name"),
        d.get("listed_date"), to_pg_timestamp(d.get("scraped_at")),
        to_pg_timestamp(d.get("first_seen")),
    )


PROPERTYFINDER_INSERT_SQL = """
    INSERT INTO propertyfinder_listings (
        property_id, reference, url, city, district,
        compound, location_full, property_type, surface_sqm, surface_sqft,
        bedrooms, bathrooms, is_furnished, available_from, title,
        price_value, price_raw, currency, price_period,
        description, amenities, images, images_count,
        avg_rent_area, avg_size_area, vs_avg_pct, vs_avg_dir,
        agent_name, agency_name, listed_date, scraped_at, first_seen
    ) VALUES %s
    ON CONFLICT (property_id) DO NOTHING
"""


def build_mktlist_row(d):
    pd = d.get("property_details") or {}
    realtor = d.get("realtor") or {}
    brokerage = d.get("brokerage") or {}
    price_val = parse_price_string(d.get("price"))
    sqft_raw = pd.get("square_footage")
    sqft_min, sqft_max, m2_approx = parse_sqft_range(sqft_raw)
    beds = parse_int_safe(d.get("beds"))
    baths = parse_int_safe(d.get("baths"))
    city, province = extract_city_province(d.get("title"), d.get("address"))
    views = parse_int_safe(d.get("views"))
    total_parking = parse_int_safe(pd.get("total_parking_spaces"))
    images = d.get("images") or []
    images = [img for img in images if img and "notavailable" not in img]
    images = to_pg_array(images)
    rooms = d.get("rooms")
    rooms_json = json.dumps(rooms) if rooms else None
    return (
        d.get("mkt_id") if d.get("mkt_id") != "No Data" else None,
        d.get("url"), d.get("address"), d.get("title"),
        city, province, pd.get("community_name"),
        pd.get("property_type"), pd.get("building_type"),
        beds, baths, sqft_raw, sqft_min, sqft_max, m2_approx,
        d.get("status"), d.get("price"), price_val, "CAD",
        d.get("description"), to_pg_array(d.get("features")),
        pd.get("parking_type"), total_parking, pd.get("heating_type"),
        pd.get("cooling"), pd.get("flooring"), pd.get("exterior_finish"),
        pd.get("basement_type"), pd.get("building_amenities"),
        pd.get("appliances_included"), images,
        len(images) if images else 0, rooms_json,
        realtor.get("name"), realtor.get("phone"), realtor.get("website"),
        brokerage.get("name"), brokerage.get("url"),
        views, d.get("added_on"), to_pg_timestamp(d.get("scraped_at")),
    )


MKTLIST_INSERT_SQL = """
    INSERT INTO mktlist_listings (
        mkt_id, url, address, title, city,
        province, community_name, property_type, building_type,
        beds, baths, square_footage_raw, surface_sqft_min,
        surface_sqft_max, surface_m2_approx, status,
        price_raw, price_value, currency,
        description, features, parking_type, total_parking,
        heating_type, cooling, flooring, exterior_finish,
        basement_type, building_amenities, appliances,
        images, images_count, rooms,
        realtor_name, realtor_phone, realtor_website,
        brokerage_name, brokerage_url,
        views, added_on, scraped_at
    ) VALUES %s
    ON CONFLICT (url) DO NOTHING
"""

SOURCE_BUILDERS = {
    "bienici": (build_bienici_row, BIENICI_INSERT_SQL),
    "mubawab": (build_mubawab_row, MUBAWAB_INSERT_SQL),
    "propertyfinder": (build_propertyfinder_row, PROPERTYFINDER_INSERT_SQL),
    "mktlist": (build_mktlist_row, MKTLIST_INSERT_SQL),
}


# ============================================================
# SCHEMA CREATION — PostgreSQL tables + archive tables
# ============================================================

SCHEMA_SQL = """
-- ─── Bien'ici (France) ──────────────────────────────────
CREATE TABLE IF NOT EXISTS bienici_listings (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(100) UNIQUE NOT NULL,
    reference VARCHAR(100),
    url TEXT,
    city VARCHAR(200),
    postal_code VARCHAR(10),
    department_code VARCHAR(5),
    district_name VARCHAR(200),
    insee_code VARCHAR(10),
    address_known BOOLEAN DEFAULT FALSE,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    property_type VARCHAR(50),
    surface_m2 DOUBLE PRECISION,
    floor INTEGER,
    rooms INTEGER,
    bedrooms INTEGER,
    bathrooms INTEGER,
    shower_rooms INTEGER DEFAULT 0,
    terraces INTEGER DEFAULT 0,
    balconies INTEGER DEFAULT 0,
    parking_spots INTEGER DEFAULT 0,
    cellars INTEGER DEFAULT 0,
    is_new BOOLEAN DEFAULT FALSE,
    is_furnished BOOLEAN DEFAULT FALSE,
    is_accessible BOOLEAN DEFAULT FALSE,
    has_elevator BOOLEAN DEFAULT FALSE,
    heating VARCHAR(100),
    optical_fiber VARCHAR(50),
    price DOUBLE PRECISION,
    currency VARCHAR(5) DEFAULT 'EUR',
    charges DOUBLE PRECISION,
    agency_fee DOUBLE PRECISION,
    price_decreased BOOLEAN DEFAULT FALSE,
    rent_excluding_charges DOUBLE PRECISION,
    price_per_m2 DOUBLE PRECISION,
    energy_class VARCHAR(5),
    ghg_class VARCHAR(5),
    energy_value INTEGER,
    ghg_value INTEGER,
    energy_diag_date VARCHAR(50),
    min_energy_cost DOUBLE PRECISION,
    max_energy_cost DOUBLE PRECISION,
    energy_numeric INTEGER,
    ghg_numeric INTEGER,
    description TEXT,
    photos TEXT[],
    photos_count INTEGER DEFAULT 0,
    virtual_tours TEXT[],
    posted_by_pro BOOLEAN DEFAULT TRUE,
    agency_name VARCHAR(300),
    agency_id VARCHAR(100),
    is_exclusive BOOLEAN DEFAULT FALSE,
    surface_per_room DOUBLE PRECISION,
    surface_per_bedroom DOUBLE PRECISION,
    equipment_score INTEGER DEFAULT 0,
    cleaned_at TIMESTAMPTZ,
    synced_at TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Mubawab (Tunisia) ─────────────────────────────────
CREATE TABLE IF NOT EXISTS mubawab_listings (
    id SERIAL PRIMARY KEY,
    ad_id VARCHAR(50) UNIQUE NOT NULL,
    url TEXT,
    city VARCHAR(200),
    country VARCHAR(5) DEFAULT 'TN',
    location_text VARCHAR(500),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    property_type VARCHAR(100),
    area_m2 DOUBLE PRECISION,
    rooms INTEGER,
    bedrooms INTEGER,
    bathrooms INTEGER,
    title VARCHAR(500),
    price DOUBLE PRECISION,
    currency VARCHAR(5) DEFAULT 'TND',
    description TEXT,
    features TEXT[],
    property_condition VARCHAR(100),
    property_age VARCHAR(100),
    floor_number VARCHAR(50),
    orientation VARCHAR(50),
    floor_type VARCHAR(100),
    images TEXT[],
    images_count INTEGER DEFAULT 0,
    seller_name VARCHAR(300),
    seller_type VARCHAR(50),
    scraped_at TIMESTAMPTZ,
    synced_at TIMESTAMPTZ DEFAULT NOW()
);

-- ─── PropertyFinder (Egypt) ─────────────────────────────
CREATE TABLE IF NOT EXISTS propertyfinder_listings (
    id SERIAL PRIMARY KEY,
    property_id VARCHAR(50) UNIQUE NOT NULL,
    reference VARCHAR(100),
    url TEXT,
    city VARCHAR(200),
    district VARCHAR(200),
    compound VARCHAR(300),
    location_full VARCHAR(500),
    property_type VARCHAR(100),
    surface_sqm INTEGER,
    surface_sqft INTEGER,
    bedrooms INTEGER,
    bathrooms INTEGER,
    is_furnished BOOLEAN,
    available_from VARCHAR(50),
    title VARCHAR(500),
    price_value DOUBLE PRECISION,
    price_raw VARCHAR(100),
    currency VARCHAR(5) DEFAULT 'EGP',
    price_period VARCHAR(20) DEFAULT 'monthly',
    description TEXT,
    amenities TEXT[],
    images TEXT[],
    images_count INTEGER DEFAULT 0,
    avg_rent_area DOUBLE PRECISION,
    avg_size_area INTEGER,
    vs_avg_pct INTEGER,
    vs_avg_dir VARCHAR(10),
    agent_name VARCHAR(300),
    agency_name VARCHAR(300),
    listed_date VARCHAR(50),
    scraped_at TIMESTAMPTZ,
    first_seen TIMESTAMPTZ,
    synced_at TIMESTAMPTZ DEFAULT NOW()
);

-- ─── MktList (Canada) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS mktlist_listings (
    id SERIAL PRIMARY KEY,
    mkt_id VARCHAR(50),
    url TEXT UNIQUE NOT NULL,
    address VARCHAR(500),
    title VARCHAR(500),
    city VARCHAR(200),
    province VARCHAR(100),
    community_name VARCHAR(200),
    property_type VARCHAR(100),
    building_type VARCHAR(100),
    beds INTEGER,
    baths INTEGER,
    square_footage_raw VARCHAR(100),
    surface_sqft_min INTEGER,
    surface_sqft_max INTEGER,
    surface_m2_approx INTEGER,
    status VARCHAR(50),
    price_raw VARCHAR(100),
    price_value DOUBLE PRECISION,
    currency VARCHAR(5) DEFAULT 'CAD',
    description TEXT,
    features TEXT[],
    parking_type VARCHAR(100),
    total_parking INTEGER,
    heating_type VARCHAR(100),
    cooling VARCHAR(100),
    flooring VARCHAR(200),
    exterior_finish VARCHAR(200),
    basement_type VARCHAR(100),
    building_amenities VARCHAR(500),
    appliances VARCHAR(500),
    images TEXT[],
    images_count INTEGER DEFAULT 0,
    rooms JSONB,
    realtor_name VARCHAR(300),
    realtor_phone VARCHAR(50),
    realtor_website TEXT,
    brokerage_name VARCHAR(300),
    brokerage_url TEXT,
    views INTEGER,
    added_on VARCHAR(50),
    scraped_at TIMESTAMPTZ,
    synced_at TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Indexes ────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_bienici_city ON bienici_listings(city);
CREATE INDEX IF NOT EXISTS idx_bienici_dept ON bienici_listings(department_code);
CREATE INDEX IF NOT EXISTS idx_bienici_price ON bienici_listings(price);
CREATE INDEX IF NOT EXISTS idx_mubawab_city ON mubawab_listings(city);
CREATE INDEX IF NOT EXISTS idx_mubawab_price ON mubawab_listings(price);
CREATE INDEX IF NOT EXISTS idx_pf_city ON propertyfinder_listings(city);
CREATE INDEX IF NOT EXISTS idx_pf_price ON propertyfinder_listings(price_value);
CREATE INDEX IF NOT EXISTS idx_mktlist_city ON mktlist_listings(city);
CREATE INDEX IF NOT EXISTS idx_mktlist_price ON mktlist_listings(price_value);
"""


def ensure_schema(pg_conn):
    """Create main tables if they don't exist."""
    cur = pg_conn.cursor()
    cur.execute(SCHEMA_SQL)
    pg_conn.commit()
    log.info("✅ Main tables ensured")


def ensure_archive_tables(pg_conn):
    """Create archive tables — structure mirrors main but with NO unique constraints."""
    cur = pg_conn.cursor()
    for name, cfg in SOURCES.items():
        main_table = cfg["pg_table"]
        archive_table = cfg["pg_archive"]

        cur.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
            (archive_table,)
        )
        if cur.fetchone()[0]:
            continue

        # Create archive table: same columns but NO unique constraints
        # This avoids the "LIKE INCLUDING ALL" bug where unique indexes get copied
        cur.execute(f"""
            CREATE TABLE {archive_table} (
                LIKE {main_table} INCLUDING DEFAULTS INCLUDING GENERATED
            )
        """)
        # Add archive-specific columns
        cur.execute(f"""
            ALTER TABLE {archive_table}
                ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ DEFAULT NOW(),
                ADD COLUMN IF NOT EXISTS archive_reason VARCHAR(50) DEFAULT 'listing_removed'
        """)
        # Drop any unique constraints that were inherited
        cur.execute(f"""
            SELECT conname FROM pg_constraint
            WHERE conrelid = '{archive_table}'::regclass
            AND contype IN ('u', 'p')
            AND conname != '{archive_table}_pkey'
        """)
        for row in cur.fetchall():
            cur.execute(f"ALTER TABLE {archive_table} DROP CONSTRAINT IF EXISTS {row[0]}")

        log.info(f"  Created archive table: {archive_table}")

    pg_conn.commit()


# ============================================================
# SYNC: MongoDB -> PostgreSQL
# ============================================================

def get_existing_ids(pg_conn, table, unique_col):
    """Load existing IDs from PG — all as strings for consistent comparison."""
    cur = pg_conn.cursor()
    cur.execute(f"SELECT {unique_col} FROM {table} WHERE {unique_col} IS NOT NULL")
    return {str(row[0]) for row in cur.fetchall()}


def sync_source(name, mongo_client, pg_conn):
    cfg = SOURCES[name]
    build_row, insert_sql = SOURCE_BUILDERS[name]

    mongo_db = mongo_client[cfg["mongo_db"]]
    mongo_col = mongo_db[cfg["mongo_col"]]

    stats = {"total_mongo": 0, "already_in_pg": 0, "new_synced": 0, "errors": 0}
    stats["total_mongo"] = mongo_col.count_documents({})

    existing_ids = get_existing_ids(pg_conn, cfg["pg_table"], cfg["pg_unique"])
    stats["already_in_pg"] = len(existing_ids)

    log.info(f"  MongoDB: {stats['total_mongo']} | PostgreSQL: {stats['already_in_pg']}")

    if stats["total_mongo"] == 0:
        log.info(f"  No documents in MongoDB — skipping")
        return stats

    mongo_unique = cfg["mongo_unique"]
    batch = []

    for doc in mongo_col.find({}, batch_size=BATCH_SIZE):
        doc_id = doc.get(mongo_unique)
        if not doc_id:
            continue

        # FIX: consistent string comparison
        if str(doc_id) in existing_ids:
            continue

        try:
            row = build_row(doc)
            batch.append(row)
        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                log.warning(f"  Row build error ({mongo_unique}={doc_id}): {e}")

        if len(batch) >= BATCH_SIZE:
            inserted = _flush_batch(pg_conn, insert_sql, batch, stats)
            batch = []
            log.info(f"  Synced {stats['new_synced']} so far...")

    if batch:
        _flush_batch(pg_conn, insert_sql, batch, stats)

    return stats


def _flush_batch(pg_conn, insert_sql, batch, stats):
    """Insert a batch into PG with error handling."""
    try:
        cur = pg_conn.cursor()
        execute_values(cur, insert_sql, batch)
        pg_conn.commit()
        stats["new_synced"] += len(batch)
    except Exception as e:
        pg_conn.rollback()
        log.error(f"  Batch insert error: {e}")
        # Try inserting one by one to find bad rows
        recovered = 0
        for row in batch:
            try:
                cur = pg_conn.cursor()
                execute_values(cur, insert_sql, [row])
                pg_conn.commit()
                recovered += 1
            except Exception:
                pg_conn.rollback()
                stats["errors"] += 1
        stats["new_synced"] += recovered
        if recovered > 0:
            log.info(f"  Recovered {recovered}/{len(batch)} from failed batch")


# ============================================================
# ARCHIVE: Check if listings still live on source websites
# ============================================================

def check_url_alive(url):
    """HEAD request to check if a listing URL is still active."""
    try:
        resp = requests.head(
            url, headers=CHECK_HEADERS,
            timeout=ARCHIVE_REQUEST_TIMEOUT, allow_redirects=True
        )
        # Definitely dead
        if resp.status_code in (404, 410):
            return False

        # Check for redirect to homepage (listing removed)
        final_url = resp.url.rstrip("/")
        if final_url in HOMEPAGE_REDIRECTS:
            return False

        # Rate limited — assume alive to be safe
        if resp.status_code == 403:
            return True

        # Any 2xx/3xx = alive
        if resp.status_code < 400:
            return True

        # 5xx = server error, assume alive
        if resp.status_code >= 500:
            return True

        return False
    except requests.RequestException:
        # Network error — assume alive (don't archive on transient failures)
        return True


def archive_listing(pg_conn, main_table, archive_table, unique_col, unique_val,
                    reason="listing_removed"):
    """Move a listing from main to archive table."""
    cur = pg_conn.cursor()
    try:
        # Get column list from main table (exclude archive-specific columns)
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND column_name NOT IN ('archived_at', 'archive_reason')
            ORDER BY ordinal_position
        """, (main_table,))
        columns = [r[0] for r in cur.fetchall()]
        cols_str = ", ".join(columns)

        # Copy to archive
        cur.execute(f"""
            INSERT INTO {archive_table} ({cols_str}, archived_at, archive_reason)
            SELECT {cols_str}, NOW(), %s
            FROM {main_table}
            WHERE {unique_col} = %s
        """, (reason, unique_val))

        # Remove from main
        cur.execute(f"DELETE FROM {main_table} WHERE {unique_col} = %s", (unique_val,))
        pg_conn.commit()
        return True
    except Exception as e:
        pg_conn.rollback()
        log.error(f"  Archive error for {unique_val}: {e}")
        return False


def archive_check_source(name, pg_conn):
    """Check all listings for a source and archive dead ones."""
    cfg = SOURCES[name]
    stats = {"checked": 0, "alive": 0, "archived": 0, "errors": 0}

    cur = pg_conn.cursor(cursor_factory=RealDictCursor)

    url_field = cfg["url_field"]
    unique_col = cfg["pg_unique"]
    table = cfg["pg_table"]
    archive_table = cfg["pg_archive"]

    cur.execute(f"SELECT {unique_col}, {url_field} FROM {table} WHERE {url_field} IS NOT NULL")
    rows = cur.fetchall()
    total = len(rows)
    log.info(f"  {total} listings to check")

    for i, row in enumerate(rows):
        unique_val = row[unique_col]
        url = row.get(url_field)

        if not url:
            continue

        alive = check_url_alive(url)
        stats["checked"] += 1

        if alive:
            stats["alive"] += 1
        else:
            success = archive_listing(pg_conn, table, archive_table, unique_col, unique_val)
            if success:
                stats["archived"] += 1
                log.info(f"  📦 Archived: {unique_val}")
            else:
                stats["errors"] += 1

        if (i + 1) % 50 == 0:
            log.info(f"  Progress: {i+1}/{total} | alive={stats['alive']} archived={stats['archived']}")

        time.sleep(ARCHIVE_REQUEST_DELAY)

    return stats


# ============================================================
# MAIN CYCLE
# ============================================================

def run_cycle(mongo_client, pg_conn, sync=True, archive=True):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"\n{'='*60}")
    log.info(f"CYCLE START: {now}")
    log.info(f"{'='*60}")

    ensure_schema(pg_conn)
    ensure_archive_tables(pg_conn)

    if sync:
        log.info(f"\n--- PHASE 1: SYNC MongoDB -> PostgreSQL ---\n")
        for name, cfg in SOURCES.items():
            log.info(f"[{cfg['flag']}] {name.upper()}")
            try:
                stats = sync_source(name, mongo_client, pg_conn)
                log.info(f"  ✅ +{stats['new_synced']} new | {stats['errors']} errors")
            except Exception as e:
                log.error(f"  ❌ FAILED: {e}")
                import traceback
                traceback.print_exc()

    if archive:
        log.info(f"\n--- PHASE 2: ARCHIVE CHECK ---\n")
        for name, cfg in SOURCES.items():
            log.info(f"[{cfg['flag']}] {name.upper()}")
            try:
                stats = archive_check_source(name, pg_conn)
                log.info(f"  ✅ {stats['checked']} checked | "
                         f"{stats['alive']} alive | "
                         f"{stats['archived']} archived | "
                         f"{stats['errors']} errors")
            except Exception as e:
                log.error(f"  ❌ FAILED: {e}")

    # Summary
    log.info(f"\n--- SUMMARY ---")
    cur = pg_conn.cursor()
    for name, cfg in SOURCES.items():
        try:
            cur.execute(f"SELECT COUNT(*) FROM {cfg['pg_table']}")
            active = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {cfg['pg_archive']}")
            archived = cur.fetchone()[0]
            log.info(f"  [{cfg['flag']}] {name}: {active} active | {archived} archived")
        except Exception:
            pg_conn.rollback()
            log.info(f"  [{cfg['flag']}] {name}: table not ready yet")

    log.info(f"\n✅ CYCLE COMPLETE\n")


def main():
    parser = argparse.ArgumentParser(description="MongoDB -> PostgreSQL Sync + Archive")
    parser.add_argument("--sync-only", action="store_true")
    parser.add_argument("--archive-only", action="store_true")
    parser.add_argument("--once", action="store_true", help="Run once, no loop")
    args = parser.parse_args()

    do_sync = not args.archive_only
    do_archive = not args.sync_only

    log.info("Connecting to MongoDB...")
    mongo = MongoClient(MONGO_URI)
    mongo.admin.command("ping")
    log.info("✅ MongoDB connected")

    log.info("Connecting to PostgreSQL...")
    pg = psycopg2.connect(PG_DSN)
    log.info("✅ PostgreSQL connected")

    try:
        if args.once:
            run_cycle(mongo, pg, sync=do_sync, archive=do_archive)
        else:
            while True:
                try:
                    run_cycle(mongo, pg, sync=do_sync, archive=do_archive)
                except Exception as e:
                    log.error(f"Cycle error: {e}")
                    import traceback
                    traceback.print_exc()
                    # Reconnect PG on failure
                    try:
                        pg.close()
                    except Exception:
                        pass
                    pg = psycopg2.connect(PG_DSN)

                log.info(f"💤 Sleeping {CYCLE_SLEEP_SECONDS}s until next cycle...")
                time.sleep(CYCLE_SLEEP_SECONDS)

    except KeyboardInterrupt:
        log.info("\n⚠️ Stopped by user")
    finally:
        pg.close()
        mongo.close()
        log.info("🔌 Connections closed")


if __name__ == "__main__":
    main()