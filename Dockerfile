# ============================================================
# Single container: Python + Playwright + Cron
# All scrapers run inside via cron schedule
# ============================================================

FROM mcr.microsoft.com/playwright/python:v1.52.0-noble

WORKDIR /app

# Install cron + utils
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Chromium for Playwright
RUN playwright install chromium --with-deps

# Python dependencies
COPY requirements-http.txt requirements-browser.txt ./
RUN pip install --no-cache-dir \
    -r requirements-http.txt \
    -r requirements-browser.txt

# Copy all scripts
COPY bienici_scraper_locations.py .
COPY scraper_mubawab_locations.py .
COPY scraper_mktlist_locations.py .
COPY scraper_propertyfinder_locations.py .
COPY mongo_to_postgre.py .
COPY monitor.py .
COPY bienici_cleaner_locations.py .
COPY runner.sh .

RUN chmod +x runner.sh
RUN mkdir -p /app/logs

# Cron schedule — written at runtime by entrypoint (needs env vars)
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]