#!/usr/bin/env python3
"""
Download and convert Rebrickable data from CSV to JSON and TXT,
filtering out unwanted themes, sets with only 1 part or missing images,
and non-actual minifigs (weapons, accessories, supplement packs),
while preserving async image validation and natural sorting.
"""
import csv
import json
import zipfile
import re
import asyncio
import aiohttp
import logging
from pathlib import Path
from urllib.request import urlretrieve

# ----------------------------
# Configuration
# ----------------------------
DATASETS = {
    'themes': {
        'url': 'https://cdn.rebrickable.com/media/downloads/themes.csv.zip',
        'sort_key': 'id',
        'numeric_fields': ['id', 'parent_id']
    },
    'sets': {
        'url': 'https://cdn.rebrickable.com/media/downloads/sets.csv.zip',
        'sort_key': 'set_num',
        'numeric_fields': ['year', 'theme_id', 'num_parts']
    },
    'minifigs': {
        'url': 'https://cdn.rebrickable.com/media/downloads/minifigs.csv.zip',
        'sort_key': 'fig_num',
        'numeric_fields': ['num_parts', 'theme_id']
    }
}

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"

FIELDS_ORDER = ["set_num", "name", "year", "num_parts", "image", "theme", "parent_theme"]

# Keywords to filter out non-actual minifigs
MINIFIG_EXCLUDE_KEYWORDS = ["Weapon", "Accessory", "Supplement", "Promo", "Set", "Pack"]

# Themes to exclude entirely
BAD_THEME_NAMES = {
    "Supplemental", "Promotional", "Designer Sets", "Seasonal",
    "Minifigures", "Books", "Activity Books", "Non-fiction Books",
    "SPIKE", "Clikits", "Modulex", "Control Lab", "Soft Bricks",
    "Service Packs"
}

# ----------------------------
# Logging setup
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ----------------------------
# Utility functions
# ----------------------------
def natural_sort_key(value):
    def convert(text):
        return (0, int(text)) if text.isdigit() else (1, text.lower())
    parts = re.split(r'(\d+)', str(value))
    return [convert(p) for p in parts if p]

def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    logging.info(f"Data directory ready: {DATA_DIR}")

def download_zip(url, temp_file):
    logging.info(f"Downloading from {url}...")
    urlretrieve(url, temp_file)
    logging.info(f"Downloaded to {temp_file}")

def extract_and_convert(temp_zip, dataset_name, sort_key, numeric_fields):
    logging.info(f"Extracting and processing {dataset_name} CSV...")
    with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
        csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError(f"No CSV found in ZIP for {dataset_name}")
        csv_filename = csv_files[0]
        logging.info(f"Found CSV file: {csv_filename}")

        with zip_ref.open(csv_filename) as csv_file:
            csv_text = csv_file.read().decode("utf-8")
            csv_text = csv_text.replace("||", "\n").replace("\r\n", "\n").replace("\r", "\n")

            csv_reader = csv.DictReader(csv_text.splitlines())
            data = []
            for row in csv_reader:
                for field in numeric_fields:
                    if field in row and row[field]:
                        row[field] = int(row[field]) if row[field].isdigit() else None
                data.append(row)

    logging.info(f"Extracted {len(data)} rows for {dataset_name}")
    return data, csv_reader.fieldnames

def add_theme_names(data, themes_lookup, parent_lookup):
    for item in data:
        tid = item.get("theme_id")
        item["theme"] = themes_lookup.get(tid, "") if isinstance(tid, int) else ""
        item["parent_theme"] = parent_lookup.get(tid, "") if isinstance(tid, int) else ""
    return data

def save_json(data, filename):
    out = DATA_DIR / filename
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logging.info(f"Saved JSON to {out}")

def save_txt(data, fieldnames, filename):
    out = DATA_DIR / filename
    with open(out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            lineterminator="||",
            quotechar='"',
            quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        writer.writerows(data)
    logging.info(f"Saved TXT to {out}")

def cleanup(temp_file):
    if temp_file.exists():
        temp_file.unlink()
        logging.info("Cleaned up temporary file")

# ----------------------------
# Async Image Validation
# ----------------------------
async def check_image(session, url, semaphore):
    async with semaphore:
        try:
            async with session.head(url, timeout=10) as resp:
                return url, resp.status == 200
        except Exception:
            return url, False

async def validate_images(data):
    cache_file = DATA_DIR / "image_cache.json"
    if cache_file.exists():
        with open(cache_file, 'r') as f:
            cache = json.load(f)
    else:
        cache = {}

    urls_to_check = [row["image"] for row in data if row["image"] and row["image"] not in cache]
    logging.info(f"Checking {len(urls_to_check)} new images...")

    semaphore = asyncio.Semaphore(20)
    async with aiohttp.ClientSession() as session:
        tasks = [check_image(session, url, semaphore) for url in urls_to_check]
        for coro in asyncio.as_completed(tasks):
            url, valid = await coro
            cache[url] = valid
            logging.info(f"{url} => {'OK' if valid else 'FAILED'}")

    # Filter rows with missing or invalid images
    filtered_data = [row for row in data if row.get("image") and cache.get(row["image"], False)]

    # Save cache
    with open(cache_file, 'w') as f:
        json.dump(cache, f, indent=2)

    logging.info(f"Image validation complete. {len(filtered_data)}/{len(data)} rows retained.")
    return filtered_data

# ----------------------------
# Main processing
# ----------------------------
def main():
    try:
        logging.info("=== Rebrickable Data Updater ===")
        ensure_data_dir()

        # Load themes
        temp_zip = PROJECT_ROOT / "temp_themes.zip"
        download_zip(DATASETS["themes"]["url"], temp_zip)
        themes_data, _ = extract_and_convert(temp_zip, "themes", "id", DATASETS["themes"]["numeric_fields"])
        cleanup(temp_zip)
        themes_lookup = {t["id"]: t.get("name", "") for t in themes_data}
        parent_lookup = {t["id"]: themes_lookup.get(t.get("parent_id"), "") for t in themes_data if t.get("parent_id")}
        logging.info(f"Loaded {len(themes_lookup)} themes")

        # Process sets and minifigs
        for dataset_name in ("sets", "minifigs"):
            config = DATASETS[dataset_name]
            temp_zip = PROJECT_ROOT / f"temp_{dataset_name}.zip"
            logging.info(f"Processing dataset: {dataset_name}")

            download_zip(config['url'], temp_zip)
            data, _ = extract_and_convert(temp_zip, dataset_name, config['sort_key'], config['numeric_fields'])
            cleanup(temp_zip)

            # Add theme names
            data = add_theme_names(data, themes_lookup, parent_lookup)

            # Filter sets/minifigs
            if dataset_name == "sets":
                data = [
                    row for row in data
                    if row.get("theme") not in BAD_THEME_NAMES
                    and row.get("num_parts") and row["num_parts"] > 1
                    and row.get("img_url")
                ]
            else:
                data = [
                    row for row in data
                    if row.get("img_url") and
                    not any(kw.lower() in (row.get("name") or "").lower() for kw in MINIFIG_EXCLUDE_KEYWORDS)
                ]

            # Normalize for TXT/JSON
            normalized_data = []
            for row in data:
                normalized_row = {
                    "set_num": row.get("set_num") or row.get("fig_num") or "",
                    "name": row.get("name", ""),
                    "year": row.get("year", ""),
                    "num_parts": row.get("num_parts", ""),
                    "image": row.get("img_url", ""),
                    "theme": row.get("theme", ""),
                    "parent_theme": row.get("parent_theme", "")
                }
                normalized_data.append(normalized_row)

            # Sort
            sort_key = "set_num" if dataset_name == "sets" else "fig_num"
            year_key = "year" if dataset_name == "sets" else None
            normalized_data.sort(key=lambda x: (
                x.get(year_key) if year_key and isinstance(x.get(year_key), int) else float("inf"),
                natural_sort_key(x.get(sort_key, ""))
            ))

            # Async image validation
            normalized_data = asyncio.run(validate_images(normalized_data))

            logging.info(f"{dataset_name}: {len(normalized_data)} rows after filtering and validation")

            save_json(normalized_data, f"{dataset_name}.json")
            save_txt(normalized_data, FIELDS_ORDER, f"{dataset_name}.txt")

        logging.info("✓ Success! All datasets processed.")

    except Exception as e:
        logging.exception("Error during processing")
        raise

if __name__ == "__main__":
    main()
