#!/usr/bin/env python3
"""
Download and convert Rebrickable data from CSV to JSON and TXT,
including themes and parent themes, filtering out invalid images asynchronously,
with persistent cache and normalized TXT output.
"""
import asyncio
import aiohttp
import csv
import json
import zipfile
import re
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
CACHE_FILE = DATA_DIR / "img_url_cache.json"

# Desired consistent TXT field order
FIELDS_ORDER = ["set_num", "name", "year", "num_parts", "image", "theme", "parent_theme"]

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
    print(f"✓ Data directory ready: {DATA_DIR}")

def download_zip(url, temp_file):
    print(f"Downloading from {url}...")
    urlretrieve(url, temp_file)
    print(f"✓ Downloaded to {temp_file}")

def extract_and_convert(temp_zip, dataset_name, sort_key, numeric_fields):
    """Extract CSV, normalize line breaks, convert numeric fields, sort."""
    print(f"Extracting and processing {dataset_name} CSV...")
    with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
        csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError(f"No CSV found in ZIP for {dataset_name}")
        csv_filename = csv_files[0]
        print(f"✓ Found CSV file: {csv_filename}")

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

            # Sort by year then natural sort key
            data.sort(key=lambda x: (
                x.get("year") if isinstance(x.get("year"), int) else float("inf"),
                natural_sort_key(x.get(sort_key, ""))
            ))

            print(f"✓ Converted and sorted {len(data)} {dataset_name}")

    return data, csv_reader.fieldnames

def add_theme_names(data, themes_lookup, parent_lookup):
    """Attach theme and parent theme names using theme_id."""
    for item in data:
        tid = item.get("theme_id")
        item["theme"] = themes_lookup.get(tid) if isinstance(tid, int) else ""
        item["parent_theme"] = parent_lookup.get(tid) if isinstance(tid, int) else ""
    return data

# ----------------------------
# Async image validation with cache
# ----------------------------
def load_image_cache():
    if CACHE_FILE.exists():
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_image_cache(cache):
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2)

async def check_image(session, row, cache):
    url = row.get("img_url") or ""
    if not url:
        return None
    if url in cache:
        if cache[url]:
            return row
        return None
    try:
        async with session.head(url, timeout=5) as resp:
            valid = resp.status == 200
            cache[url] = valid
            if valid:
                return row
    except Exception:
        cache[url] = False
    return None

async def filter_valid_images_async(data, cache, concurrency=50):
    """Filter rows with valid images using cache and asyncio."""
    valid_data = []
    total = len(data)
    print(f"Filtering {total} rows for valid images...")

    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [check_image(session, row, cache) for row in data]
        for count, coro in enumerate(asyncio.as_completed(tasks), start=1):
            row = await coro
            if row:
                valid_data.append(row)
            if count % 50 == 0 or count == total:
                print(f"✓ Checked {count}/{total}, valid so far: {len(valid_data)}", end="\r")

    print(f"\n✓ Filtering complete, {len(valid_data)} rows with valid images remain.")
    return valid_data

# ----------------------------
# Save functions
# ----------------------------
def save_json(data, filename):
    out = DATA_DIR / filename
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved JSON to {out}")

def save_txt(data, fieldnames, filename):
    out = DATA_DIR / filename
    fieldnames = list(fieldnames)
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
    print(f"✓ Saved TXT to {out}")

def cleanup(temp_file):
    if temp_file.exists():
        temp_file.unlink()
        print("✓ Cleaned up temporary file")

# ----------------------------
# Main processing
# ----------------------------
def main():
    try:
        print("=== Rebrickable Data Updater ===\n")
        ensure_data_dir()

        # Load or initialize image cache
        img_cache = load_image_cache()

        # Step 1: Load themes
        temp_zip = PROJECT_ROOT / "temp_themes.zip"
        download_zip(DATASETS["themes"]["url"], temp_zip)
        themes_data, _ = extract_and_convert(temp_zip, "themes", "id", DATASETS["themes"]["numeric_fields"])
        cleanup(temp_zip)
        themes_lookup = {t["id"]: t.get("name", "") for t in themes_data}
        parent_lookup = {t["id"]: themes_lookup.get(t.get("parent_id")) for t in themes_data if t.get("parent_id")}
        print(f"✓ Loaded {len(themes_lookup)} themes")

        # Step 2: Process sets and minifigs
        for dataset_name in ("sets", "minifigs"):
            config = DATASETS[dataset_name]
            temp_zip = PROJECT_ROOT / f"temp_{dataset_name}.zip"
            print(f"\nProcessing dataset: {dataset_name}")

            download_zip(config['url'], temp_zip)
            data, _ = extract_and_convert(temp_zip, dataset_name, config['sort_key'], config['numeric_fields'])
            cleanup(temp_zip)

            # Add theme names
            data = add_theme_names(data, themes_lookup, parent_lookup)

            # Filter images asynchronously
            data = asyncio.run(filter_valid_images_async(data, img_cache))

            # Normalize rows for TXT/JSON
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

            print(f"{dataset_name}: {len(normalized_data)} rows remaining after filtering images")

            save_json(normalized_data, f"{dataset_name}.json")
            save_txt(normalized_data, FIELDS_ORDER, f"{dataset_name}.txt")

        # Save image cache
        save_image_cache(img_cache)
        print("\n✓ Success! All datasets processed.")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise

if __name__ == "__main__":
    main()
