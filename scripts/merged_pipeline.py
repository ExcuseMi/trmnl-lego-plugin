#!/usr/bin/env python3
"""
Complete LEGO data pipeline: Download Rebrickable data, filter and validate,
then generate options.yml and theme-based compact JSON files for TRMNL plugin.
Stores sets per theme in data/themes/{theme-slug}/0.json files under 90KB each.
"""

import csv
import datetime
import json
import zipfile
import re
import asyncio
import aiohttp
import logging
import yaml
import os
from pathlib import Path
from urllib.request import urlretrieve
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ==== Configuration ====
BRICKSET_API_KEY = os.getenv('BRICKSET_API_KEY')

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
THEME_DIR = DATA_DIR / "themes"

SETS_FILE = DATA_DIR / "sets.json"
OUTPUT_FILE = DATA_DIR / "options.yml"

FIELDS_ORDER = ["set_num", "name", "year", "num_parts", "theme", "parent_theme"]

# Max per theme JSON file size (KB)
MAX_FILE_SIZE_KB = 90

# ==== Logging ====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==== BrickSet API Functions ====
async def fetch_brickset_themes():
    """Fetch all themes from BrickSet API"""
    if not BRICKSET_API_KEY:
        logging.warning("No BRICKSET_API_KEY found in .env, skipping BrickSet theme fetch")
        return []

    url = f"https://brickset.com/api/v3.asmx/getThemes?apikey={BRICKSET_API_KEY}"

    try:
        headers = {
            'Accept': 'application/json'
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    text = await response.text()
                    data = json.loads(text)
                    themes = data.get('themes', [])
                    theme_names = sorted([t['theme'] for t in themes if 'theme' in t])
                    logging.info(f"Fetched {len(theme_names)} themes from BrickSet")
                    return theme_names
                else:
                    logging.error(f"Failed to fetch BrickSet themes: HTTP {response.status}")
                    return []
    except Exception as e:
        logging.error(f"Error fetching BrickSet themes: {e}")
        return []


# ==== Utility Functions ====
def slugify(text):
    """Convert text to URL-safe slug"""
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text.strip('-')


def natural_sort_key(value):
    def convert(text):
        return (0, int(text)) if text.isdigit() else (1, text.lower())
    parts = re.split(r'(\d+)', str(value))
    return [convert(p) for p in parts if p]


def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    THEME_DIR.mkdir(parents=True, exist_ok=True)
    logging.info(f"Data directories ready: {DATA_DIR}, {THEME_DIR}")


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


def cleanup(temp_file):
    if temp_file.exists():
        temp_file.unlink()
        logging.info("Cleaned up temporary file")


# ==== Async Image Validation ====
async def check_image(session, url, semaphore):
    async with semaphore:
        try:
            async with session.head(url, timeout=10) as resp:
                return url, resp.status == 200
        except Exception:
            return url, False


async def validate_images(data):
    """Validate images and filter out rows with missing images"""
    cache_file = DATA_DIR / "image_cache.json"
    cache = json.load(open(cache_file)) if cache_file.exists() else {}

    urls_to_check = []
    set_to_url = {}
    for row in data:
        sn = row.get("set_num")
        if sn:
            url = f"https://cdn.rebrickable.com/media/sets/{sn}.jpg"
            set_to_url[sn] = url
            if url not in cache:
                urls_to_check.append(url)

    if urls_to_check:
        semaphore = asyncio.Semaphore(20)
        async with aiohttp.ClientSession() as session:
            tasks = [check_image(session, url, semaphore) for url in urls_to_check]
            for coro in asyncio.as_completed(tasks):
                url, valid = await coro
                cache[url] = valid

    filtered = []
    for row in data:
        sn = row.get("set_num")
        url = set_to_url.get(sn)
        if url and cache.get(url, False):
            row.pop("image", None)
            filtered.append(row)

    with open(cache_file, "w") as f:
        json.dump(cache, f, indent=2)

    return filtered

async def download_and_process_rebrickable():
    """Download and process Rebrickable data"""
    logging.info("=== Phase 1: Download & Process Rebrickable Data ===")

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

        data = add_theme_names(data, themes_lookup, parent_lookup)


        # Normalize - Keep Rebrickable theme names
        normalized_data = []
        for row in data:
            normalized_row = {
                "set_num": row.get("set_num") or row.get("fig_num") or "",
                "name": row.get("name", ""),
                "year": row.get("year", ""),
                "num_parts": row.get("num_parts", ""),
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

        # Validate images
        normalized_data = await validate_images(normalized_data)

        logging.info(f"{dataset_name}: {len(normalized_data)} rows after filtering and validation")

        save_json(normalized_data, f"{dataset_name}.json")


# ==== Theme-Based File Creation ====
def create_theme_files(sets):
    """Create separate JSON files per theme stored as data/themes/{slug}/0.json under 90KB"""

    theme_sets = {}
    for s in sets:
        theme_sets.setdefault(s.get("theme", "Unknown"), []).append(s)

    theme_info = {}

    for theme_name, theme_set_list in theme_sets.items():
        slug = slugify(theme_name)
        theme_path = THEME_DIR / slug
        theme_path.mkdir(parents=True, exist_ok=True)
        output_file = theme_path / "0.json"

        fields = ["set_num", "name", "year", "num_parts", "theme", "parent_theme"]

        theme_set_list.sort(key=lambda x: (
            x.get("year") if isinstance(x.get("year"), int) else float("inf"),
            natural_sort_key(x.get("set_num", ""))
        ))

        compact = [fields] + [
            [s.get(f, "") for f in fields] for s in theme_set_list
        ]

        json_str = json.dumps(compact, separators=(",", ":"), ensure_ascii=False)
        size_kb = len(json_str.encode("utf-8")) / 1024

        if size_kb > MAX_FILE_SIZE_KB:
            left, right = 1, len(theme_set_list)
            max_sets = 1
            while left <= right:
                mid = (left + right) // 2
                test_compact = [fields] + [[s.get(f, "") for f in fields] for s in theme_set_list[:mid]]
                kb = len(json.dumps(test_compact, separators=(",", ":"), ensure_ascii=False).encode("utf-8")) / 1024
                if kb <= MAX_FILE_SIZE_KB:
                    max_sets = mid
                    left = mid + 1
                else:
                    right = mid - 1
            theme_set_list = theme_set_list[:max_sets]
            compact = [fields] + [[s.get(f, "") for f in fields] for s in theme_set_list]
            json_str = json.dumps(compact, separators=(",", ":"), ensure_ascii=False)

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(json_str)

        theme_info[slug] = {
            "name": theme_name,
            "count": len(theme_set_list),
            "max_file_count": 1
        }

    return theme_info


# ==== Create YAML ====
async def create_options_yml(theme_info):
    yaml.add_representer(dict, lambda dumper, data: dumper.represent_mapping("tag:yaml.org,2002:map", data.items()))

    themes_brickset = await fetch_brickset_themes()
    sorted_themes = sorted(theme_info.items(), key=lambda x: x[1]["name"])

    about_field = {
        'keyname': 'about',
        'name': 'About This Plugin',
        'field_type': 'author_bio',
        'description': "Display LEGO® sets on your TRMNL device...",
        'github_url': 'https://github.com/ExcuseMi/trmnl-lego-plugin',
        'category': 'life'
    }

    fields = [
        about_field,
        {
            'keyname': 'vendor',
            'field_type': 'select',
            'name': 'Data Source',
            'description': 'Select which LEGO® data source to use.',
            'options': [
                {'Rebrickable (Curated Dataset)': 'rebrickable'},
                {'Brickset (Live API)': 'brickset'},
            ],
            'default': 'rebrickable',
        },
        {
            'keyname': 'themes_rebrickable',
            'field_type': 'select',
            'name': f'Filter by Themes – Rebrickable ({len(sorted_themes)})',
            'multiple': True,
            'options': [
                {f"{info['name']} : {info['count']}": f"{slug}|{info['max_file_count']}"}
                for slug, info in sorted_themes
            ],
        },
        {
            'keyname': 'themes_brickset',
            'field_type': 'select',
            'name': f'Filter by Themes – Brickset ({len(themes_brickset)})',
            'multiple': True,
            'options': [{t: t} for t in themes_brickset],
        }
    ]

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        yaml.dump(fields, f, allow_unicode=True, sort_keys=False, width=1000)

    logging.info(f"✓ Created {OUTPUT_FILE.name}")


# ==== Generate Options ====
async def generate_options():
    if not SETS_FILE.exists():
        logging.error(f"Error: {SETS_FILE} not found. Run Phase 1 first.")
        return
    sets = json.load(open(SETS_FILE, 'r', encoding='utf-8'))
    theme_info = create_theme_files(sets)
    await create_options_yml(theme_info)


# ==== Main ====
async def main():
    try:
        logging.info("=" * 60)
        logging.info(" LEGO Data Pipeline for TRMNL Plugin")
        logging.info("=" * 60)

        ensure_data_dir()

        # Phase 1: Download and process Rebrickable data
        await download_and_process_rebrickable()

        # Phase 2: Generate plugin options
        await generate_options()

        logging.info("\n✓ Success! Pipeline complete. 🎉")

    except Exception as e:
        logging.exception("Error during processing")
        raise


if __name__ == "__main__":
    asyncio.run(main())
