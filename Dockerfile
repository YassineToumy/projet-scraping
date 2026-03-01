FROM mcr.microsoft.com/playwright/python:v1.52.0-noble

WORKDIR /app

# Install cron + utils
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies FIRST
COPY requirements-http.txt requirements-browser.txt ./
RUN pip install --no-cache-dir \
    -r requirements-http.txt \
    -r requirements-browser.txt

# THEN install Chromium (playwright is now available)
RUN playwright install chromium --with-deps

# Copy all scripts
COPY bienici_scraper_locations.py .
COPY bienici_cleaner_locations.py .
COPY scraper_mubawab_locations.py .
COPY scraper_mktlist_locations.py .
COPY scraper_propertyfinder_locations.py .
COPY mongo_to_postgre.py .
COPY monitor.py .
COPY runner.sh .
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x runner.sh /entrypoint.sh
RUN mkdir -p /app/logs

ENTRYPOINT ["/entrypoint.sh"]