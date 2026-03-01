"""
Mubawab.tn Scraper - Annonces de location (appartements & maisons)
Stockage dans MongoDB — config via .env
"""

import os
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import json
import time
import re
from datetime import datetime
from urllib.parse import urljoin, unquote
from dotenv import load_dotenv

load_dotenv()

# ─── Configuration — from .env ───────────────────────────────────
MONGO_URI = os.environ["MONGODB_URI"]
DB_NAME = os.getenv("MONGO_MUBAWAB_DB", "mubawab")
COLLECTION_NAME = os.getenv("MONGO_MUBAWAB_COL", "locations")

BASE_URL = "https://www.mubawab.tn"
LISTING_URL = "https://www.mubawab.tn/fr/cc/immobilier-a-louer-all:emr:2:sc:apartment-rent,house-rent"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DELAY_BETWEEN_REQUESTS = 2
MAX_PAGES = 150


# ─── MongoDB ─────────────────────────────────────────────────────
def get_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION_NAME]
    col.create_index("ad_id", unique=True)
    return col


# ─── Extraction des liens d'annonces depuis la page listing ─────
def get_ad_links_from_listing(page_url):
    resp = requests.get(page_url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    links = set()

    for box in soup.select(".listingBox[linkRef]"):
        href = box.get("linkRef", "")
        if "/fr/pa/" in href or "/fr/a/" in href:
            full_url = urljoin(BASE_URL, href)
            links.add(full_url)

    if not links:
        for a_tag in soup.select("a[href]"):
            href = a_tag.get("href", "")
            if "/fr/pa/" in href or "/fr/a/" in href:
                full_url = urljoin(BASE_URL, href)
                if re.search(r"/fr/(pa|a)/\d+/", full_url):
                    links.add(full_url)

    return list(links)


def get_page_url(base_url, page_num):
    if page_num <= 1:
        return base_url
    return f"{base_url}:p:{page_num}"


# ─── Extraction des données d'une annonce ────────────────────────
def parse_ad_page(url):
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    data = {}

    ad_id_input = soup.find("input", {"id": "adId"})
    data["ad_id"] = ad_id_input["value"] if ad_id_input else extract_id_from_url(url)
    data["url"] = url

    json_ld = extract_json_ld(soup)
    if json_ld:
        data["title"] = json_ld.get("name", "")
        data["description"] = json_ld.get("description", "")

        offers = json_ld.get("offers", {})
        data["price"] = offers.get("price")
        data["currency"] = offers.get("priceCurrency", "TND")

        item = json_ld.get("itemOffered", {})
        data["rooms"] = item.get("numberOfRooms")
        data["bedrooms"] = item.get("numberOfBedrooms")
        data["bathrooms"] = item.get("numberOfBathroomsTotal")

        floor_size = item.get("floorSize", {})
        data["area_m2"] = floor_size.get("value")

        address = item.get("address", {})
        data["city"] = address.get("addressLocality", "")
        data["country"] = address.get("addressCountry", "")

        seller = json_ld.get("seller", {})
        data["seller_name"] = seller.get("name", "")
        data["seller_type"] = seller.get("@type", "")

        images = json_ld.get("image", [])
        data["images"] = images if isinstance(images, list) else [images]
    else:
        data.update(parse_from_html(soup))

    location_tag = soup.find("h3", class_="greyTit")
    if location_tag:
        data["location_text"] = location_tag.get_text(strip=True)

    lat_input = soup.find("input", {"id": "latField"})
    lng_input = soup.find("input", {"id": "lngField"})
    if lat_input and lng_input:
        try:
            data["latitude"] = float(lat_input["value"])
            data["longitude"] = float(lng_input["value"])
        except (ValueError, KeyError):
            pass

    data["features"] = extract_features(soup)
    data["main_features"] = extract_main_features(soup)
    data["property_type"] = extract_property_type(soup)
    data["scraped_at"] = datetime.utcnow()

    return data


def extract_json_ld(soup):
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            d = json.loads(script.string)
            if d.get("@type") == "RealEstateListing":
                return d
        except (json.JSONDecodeError, TypeError):
            continue
    return None


def extract_id_from_url(url):
    m = re.search(r"/(?:pa|a)/(\d+)/", url)
    return m.group(1) if m else None


def parse_from_html(soup):
    data = {}

    h1 = soup.find("h1", class_="searchTitle")
    data["title"] = h1.get_text(strip=True) if h1 else ""

    price_tag = soup.find("h3", class_="orangeTit")
    if price_tag:
        price_text = price_tag.get_text(strip=True)
        m = re.search(r"([\d\s]+)", price_text.replace("\xa0", " "))
        if m:
            data["price"] = float(m.group(1).replace(" ", ""))
    data["currency"] = "TND"

    details = soup.select(".adDetailFeature span")
    for detail in details:
        txt = detail.get_text(strip=True)
        if "m²" in txt:
            m = re.search(r"(\d+)", txt)
            if m:
                data["area_m2"] = int(m.group(1))
        elif "Pièce" in txt:
            m = re.search(r"(\d+)", txt)
            if m:
                data["rooms"] = int(m.group(1))
        elif "Chambre" in txt:
            m = re.search(r"(\d+)", txt)
            if m:
                data["bedrooms"] = int(m.group(1))
        elif "Salle de bain" in txt:
            m = re.search(r"(\d+)", txt)
            if m:
                data["bathrooms"] = int(m.group(1))

    desc_block = soup.find("div", class_="blockProp")
    if desc_block:
        p_tag = desc_block.find("p")
        if p_tag:
            data["description"] = p_tag.get_text(separator="\n", strip=True)

    data["images"] = []
    for img in soup.select(".picturesGallery img"):
        src = img.get("src", "")
        if src and "mubawab-media" in src:
            data["images"].append(src)

    return data


def extract_features(soup):
    features = []
    for feat in soup.select(".adFeature .fSize11"):
        txt = feat.get_text(strip=True)
        if txt:
            features.append(txt)
    return features


def extract_main_features(soup):
    main_feats = {}
    for feat_block in soup.select(".adMainFeature"):
        label_el = feat_block.select_one(".adMainFeatureContentLabel")
        value_el = feat_block.select_one(".adMainFeatureContentValue")
        if label_el and value_el:
            key = label_el.get_text(strip=True).rstrip(":")
            val = value_el.get_text(strip=True)
            main_feats[key] = val
    return main_feats


def extract_property_type(soup):
    for feat_block in soup.select(".adMainFeature"):
        label_el = feat_block.select_one(".adMainFeatureContentLabel")
        value_el = feat_block.select_one(".adMainFeatureContentValue")
        if label_el and "Type de bien" in label_el.get_text():
            return value_el.get_text(strip=True) if value_el else None
    return None


# ─── Scraping principal ──────────────────────────────────────────
def scrape_all():
    collection = get_collection()
    all_links = []
    page = 1
    consecutive_no_new = 0

    print(f"🔍 Début du scraping de Mubawab.tn - Locations")
    print(f"   URL de base: {LISTING_URL}")
    print(f"   Max pages: {MAX_PAGES}\n")

    while page <= MAX_PAGES:
        page_url = get_page_url(LISTING_URL, page)
        print(f"📄 Page {page}: {page_url}")

        try:
            links = get_ad_links_from_listing(page_url)
            if not links:
                print(f"   ❌ Aucune annonce trouvée, fin de la pagination.")
                break

            new_links = []
            for link in links:
                ad_id = extract_id_from_url(link)
                if ad_id and not collection.find_one({"ad_id": ad_id}):
                    new_links.append(link)

            print(f"   ✅ {len(links)} annonces trouvées, {len(new_links)} nouvelles")
            all_links.extend(new_links)

            if len(new_links) == 0:
                consecutive_no_new += 1
                if consecutive_no_new >= 3:
                    print(f"   ⏹️  3 pages consécutives sans nouvelles annonces, arrêt.")
                    break
            else:
                consecutive_no_new = 0

        except Exception as e:
            print(f"   ⚠️  Erreur page {page}: {e}")

        page += 1
        time.sleep(DELAY_BETWEEN_REQUESTS)

    all_links = list(dict.fromkeys(all_links))
    print(f"\n📊 Total annonces à scraper: {len(all_links)}\n")

    success = 0
    errors = 0

    for i, link in enumerate(all_links, 1):
        print(f"  [{i}/{len(all_links)}] {unquote(link[:80])}...")

        try:
            ad_data = parse_ad_page(link)

            collection.update_one(
                {"ad_id": ad_data["ad_id"]},
                {"$set": ad_data},
                upsert=True
            )
            success += 1
            print(f"    ✅ {ad_data.get('title', '')[:50]} - {ad_data.get('price')} TND")

        except Exception as e:
            errors += 1
            print(f"    ❌ Erreur: {e}")

        time.sleep(DELAY_BETWEEN_REQUESTS)

    print(f"\n{'='*50}")
    print(f"✅ Scraping terminé!")
    print(f"   Succès:  {success}")
    print(f"   Erreurs: {errors}")
    print(f"   Total en base: {collection.count_documents({})}")
    print(f"{'='*50}")


if __name__ == "__main__":
    scrape_all()