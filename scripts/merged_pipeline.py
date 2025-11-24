#!/usr/bin/env python3
"""
Complete LEGO data pipeline: Download Rebrickable data, filter and validate,
then generate options.yml and compact JSON for TRMNL plugin.
Supports dual theme naming for Rebrickable and BrickSet compatibility.
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

SETS_FILE = DATA_DIR / "sets.json"
COMPACT_JSON = DATA_DIR / "reduced_sets.json"
OUTPUT_FILE = DATA_DIR / "options.yml"

FIELDS_ORDER = ["set_num", "name", "year", "num_parts", "theme", "parent_theme"]

MINIFIG_EXCLUDE_KEYWORDS = ["Weapon", "Accessory", "Supplement", "Promo", "Set", "Pack"]

BAD_THEME_NAMES = {
    "Supplemental", "Promotional", "Designer Sets", "Seasonal",
    "Minifigures", "Books", "Activity Books", "Non-fiction Books",
    "SPIKE", "Clikits", "Modulex", "Control Lab", "Soft Bricks",
    "Service Packs", "Database Sets", "Clocks and Watches", "Key Chain"
}

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
                    # Read as text first, then parse as JSON manually
                    # BrickSet returns JSON but with incorrect content-type header
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
    """Validate images and filter out rows with missing or invalid images.
    Only stores set_num in the data, not the full URL."""
    cache_file = DATA_DIR / "image_cache.json"

    if cache_file.exists():
        with open(cache_file, 'r') as f:
            cache = json.load(f)
        logging.info(f"Loaded cache with {len(cache)} entries")
    else:
        cache = {}

    # Build URLs from set_num for validation
    urls_to_check = []
    set_num_to_url = {}
    for row in data:
        set_num = row.get("set_num")
        if set_num:
            url = f"https://cdn.rebrickable.com/media/sets/{set_num}.jpg"
            set_num_to_url[set_num] = url
            if url not in cache:
                urls_to_check.append(url)

    logging.info(f"Checking {len(urls_to_check)} new images...")

    if urls_to_check:
        semaphore = asyncio.Semaphore(20)
        async with aiohttp.ClientSession() as session:
            tasks = [check_image(session, url, semaphore) for url in urls_to_check]
            checked = 0
            for coro in asyncio.as_completed(tasks):
                url, valid = await coro
                cache[url] = valid
                checked += 1
                if checked % 100 == 0:
                    logging.info(f"Progress: {checked}/{len(urls_to_check)} images checked")

    # Filter data - only keep rows with valid images, store only set_num
    filtered_data = []
    removed_count = 0
    for row in data:
        set_num = row.get("set_num")
        if set_num:
            url = set_num_to_url.get(set_num)
            if url and cache.get(url, False):
                # Remove the full URL, keep only set_num (image will be reconstructed later)
                row.pop("image", None)
                filtered_data.append(row)
            else:
                removed_count += 1
        else:
            removed_count += 1

    with open(cache_file, 'w') as f:
        json.dump(cache, f, indent=2)

    logging.info(f"Image validation complete.")
    logging.info(f"Retained: {len(filtered_data)} rows")
    logging.info(f"Removed: {removed_count} rows (invalid/missing images)")

    return filtered_data


# ==== Data Download & Processing ====
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

        # Filter
        if dataset_name == "sets":
            data = [
                row for row in data
                if row.get("theme") not in BAD_THEME_NAMES
                   and row.get("parent_theme") not in BAD_THEME_NAMES
                   and row.get("num_parts") and row["num_parts"] > 1
                   and row.get("img_url")
            ]
        else:
            data = [
                row for row in data
                if row.get("img_url") and
                   not any(kw.lower() in (row.get("name") or "").lower() for kw in MINIFIG_EXCLUDE_KEYWORDS)
            ]

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


# ==== Quality Filtering ====
def filter_quality_sets(sets, target_count=8000):
    excluded_themes = {
        'Service Packs', 'Promotional', 'Seasonal', 'Books', 'Gear',
        'Key Chain', 'Magnets', 'Pins', 'Stickers', 'Card Holder'
    }
    excluded_parent_themes = {'Promotional', 'Gear', 'Books'}

    filtered = [
        s for s in sets
        if s.get('num_parts', 0) >= 20
           and s.get('year', 0) >= 1970
           and s.get('theme') not in excluded_themes
           and s.get('parent_theme') not in excluded_parent_themes
    ]

    logging.info(f"After initial filtering: {len(filtered)} sets")

    if len(filtered) > target_count:
        def score(s):
            score = min(s.get("num_parts", 0), 2000) / 10

            year = s.get("year", 1970)
            if year >= 2020:
                score += 500
            elif year >= 2010:
                score += 300
            elif year >= 2000:
                score += 150
            elif year >= 1990:
                score += 50

            popular = {
                'Star Wars', 'City', 'Creator', 'Technic', 'Friends',
                'Ninjago', 'Harry Potter', 'Marvel', 'DC', 'Architecture',
                'Ideas', 'Castle', 'Space', 'Pirates', 'Trains'
            }
            if s.get("theme") in popular or s.get("parent_theme") in popular:
                score += 200

            return score

        filtered = sorted(filtered, key=score, reverse=True)[:target_count]
        logging.info(f"Reduced to top {target_count} sets")

    return filtered


# ==== Compact JSON Creation ====
def create_compact_json(sets):
    fields = ["set_num", "name", "year", "num_parts", "theme", "parent_theme"]
    compact = [fields] + [
        [
            s.get("set_num", ""), s.get("name", ""), s.get("year", ""),
            s.get("num_parts", ""), s.get("theme", ""), s.get("parent_theme", "")
        ]
        for s in sets
    ]

    with open(COMPACT_JSON, "w", encoding="utf-8") as f:
        json.dump(compact, f, separators=(",", ":"), ensure_ascii=False)

    size_mb = COMPACT_JSON.stat().st_size / (1024 * 1024)
    logging.info(f"Created {COMPACT_JSON.name}: {size_mb:.2f} MB")


# ==== Extract Themes ====
def extract_themes(sets):
    return (
        sorted({s.get("theme") for s in sets if s.get("theme")}),
        sorted({s.get("parent_theme") for s in sets if s.get("parent_theme")})
    )


# ==== Create YAML ====
async def create_options_yml(filtered_sets, themes_rebrickable, parent_themes_rebrickable):
    yaml.add_representer(dict, lambda dumper, data: dumper.represent_mapping("tag:yaml.org,2002:map", data.items()))

    min_year = min(s.get('year', 9999) for s in filtered_sets)
    max_year = max(s.get('year', 0) for s in filtered_sets)
    min_parts = min(s.get('num_parts', 9999) for s in filtered_sets)
    max_parts = max(s.get('num_parts', 0) for s in filtered_sets)

    # Fetch BrickSet themes if API key is available
    themes_brickset = await fetch_brickset_themes()

    about_field = {
        'keyname': 'about',
        'name': 'About This Plugin',
        'field_type': 'author_bio',
        'description':
            f"Display LEGO sets on your TRMNL device with filtering options.<br /><br />"
            f"<strong>Two Data Modes:</strong><br />"
            f"● <strong>Curated Dataset (default):</strong> {len(filtered_sets):,} pre-selected sets from <a href='https://rebrickable.com/'>Rebrickable.com</a> (non-LEGO items, micro sets, and sets without images excluded)<br />"
            f"● <strong>BrickSet API Mode:</strong> Live data from <a href='https://brickset.com/'>BrickSet.com</a> with access to your collection (requires free API key)<br /><br />"
            f"<strong>BrickSet Setup (Optional):</strong><br />"
            f"1. Create a free account at <a href='https://brickset.com/signup'>brickset.com/signup</a><br />"
            f"2. Request your API key at <a href='https://brickset.com/tools/webservices/requestkey'>brickset.com/tools/webservices/requestkey</a><br />"
            f"3. Enter your API key below to enable BrickSet mode<br />"
            f"4. (Optional) To show sets you own/want: call the login API method to get your userHash - see <a href='https://brickset.com/article/52664/api-version-3-documentation'>API docs</a> (search for 'login' method)<br /><br />"
            f"<strong>Note:</strong> Each data mode has its own theme filter since Rebrickable and BrickSet use different theme naming conventions.",
        'github_url': 'https://github.com/ExcuseMi/trmnl-lego-plugin',
        'category': 'life'
    }

    fields = [
        about_field,
        {
            'keyname': 'brickset_api_key',
            'field_type': 'string',
            'name': 'BrickSet API Key (Optional)',
            'description': 'Enter your BrickSet API key to use live data from BrickSet instead of the curated dataset. Get your key from <a href="https://brickset.com/tools/webservices/requestkey" target="_blank">brickset.com/tools/webservices/requestkey</a>',
            'optional': True,
            'placeholder': 'Your API key'
        },
        {
            'keyname': 'brickset_user_hash',
            'field_type': 'string',
            'name': 'BrickSet User Hash (Optional)',
            'description': 'Enter your user hash to filter by sets you own or want. To get it: call the BrickSet login API with your credentials. See the <a href="https://brickset.com/article/52664/api-version-3-documentation" target="_blank">login method documentation</a>. Example: <code>https://brickset.com/api/v3.asmx/login?apiKey=YOUR_KEY&username=YOUR_USERNAME&password=YOUR_PASSWORD</code>',
            'optional': True,
            'placeholder': 'Your user hash from login API response'
        },
        {
            'keyname': 'show_qr_code',
            'name': 'Show QR Code',
            'field_type': 'select',
            'description': 'Display a QR code that links to the set details page',
            'options': [
                {'Hide QR Code': 'hide'},
                {'Show QR Code (Rebrickable)': 'show_rebrickable'},
                {'Show QR Code (BrickSet)': 'show_brickset'},
            ],
            'default': 'hide',
            'optional': True
        },
        {
            'keyname': 'brickset_pricing',
            'field_type': 'select',
            'name': 'Show Set Price (BrickSet Only)',
            'description': '<strong>BrickSet mode only:</strong> Show the Lego Sets retail price from LEGO.com',
            'options': [
                {'No pricing': ''},
                {'CA': 'CA'},
                {'DE': 'DE'},
                {'UK': 'UK'},
                {'US': 'US'},
            ],
            'default': '',
            'optional': True
        },
        {
            'keyname': 'brickset_show_minifigs_included',
            'field_type': 'select',
            'name': 'Show minifigs included (BrickSet Only)',
            'description': '<strong>BrickSet mode only:</strong> Show the number of minifigs included',
            'options': [
                {'Yes': 'yes'},
                {'No': 'no'},
            ],
            'default': 'no',
            'optional': True
        },
        {
            'keyname': 'themes_rebrickable',
            'field_type': 'select',
            'name': f'Filter by Themes - Rebrickable ({len(themes_rebrickable)})',
            'description': f'<strong>For Curated Dataset mode:</strong> Select themes to filter from {len(filtered_sets):,} pre-selected sets. Uses Rebrickable theme names.',
            'multiple': True,
            'options': [{t: t} for t in themes_rebrickable],
            'optional': True
        },
        {
            'keyname': 'themes_brickset',
            'field_type': 'select',
            'name': f'Filter by Themes - BrickSet ({len(themes_brickset)})',
            'description': '<strong>For BrickSet API mode:</strong> Select themes for server-side filtering. Uses BrickSet theme names. Only applies when BrickSet API key is provided.',
            'multiple': True,
            'options': [{t: t} for t in themes_brickset],
            'optional': True
        },
        {
            'keyname': 'min_year',
            'field_type': 'number',
            'name': 'Minimum Release Year',
            'description': '<strong>Curated mode:</strong> Filters the curated dataset<br /><strong>BrickSet mode:</strong> Server-side filtering (only fetches matching sets)',
            'min': 1900,
            'optional': True,
            'placeholder': f"{min_year}"
        },
        {
            'keyname': 'max_year',
            'field_type': 'number',
            'name': 'Maximum Release Year',
            'description': '<strong>Curated mode:</strong> Filters the curated dataset<br /><strong>BrickSet mode:</strong> Server-side filtering (only fetches matching sets)',
            'min': 1900,
            'optional': True,
            'placeholder': f'{datetime.date.today().year}'
        },
        {
            'keyname': 'brickset_owned_wanted',
            'field_type': 'select',
            'name': 'Show Owned and/or Wanted Sets (BrickSet Only)',
            'description': '<strong>BrickSet mode only:</strong> Filter sets based on your collection. Requires user hash to be provided above.',
            'options': [
                {'All Sets': ''},
                {'Only Owned Sets': '%27owned%27:1,'},
                {'Only Wanted Sets': '%27wanted%27:1,'},
            ],
            'default': '',
            'optional': True
        },
        {
            'keyname': 'min_parts',
            'field_type': 'number',
            'name': 'Minimum Number of Parts',
            'description': 'Only display sets with at least this many pieces (filtered on your device in both modes)',
            'min': 0,
            'optional': True,
            'placeholder': "1000"
        },
        {
            'keyname': 'max_parts',
            'field_type': 'number',
            'name': 'Maximum Number of Parts',
            'description': 'Only display sets with at most this many pieces (filtered on your device in both modes)',
            'min': 0,
            'optional': True,
            'placeholder': f'{max_parts}'
        }
    ]

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        yaml.dump(fields, f, allow_unicode=True, sort_keys=False, width=1000)

    logging.info(f"✓ Created {OUTPUT_FILE.name}")


# ==== Generate Options ====
async def generate_options():
    """Generate options.yml and compact JSON from sets.json"""
    logging.info("=== Phase 2: Generate Plugin Options ===")

    if not SETS_FILE.exists():
        logging.error(f"Error: {SETS_FILE} not found. Run Phase 1 first.")
        return

    with open(SETS_FILE, 'r', encoding='utf-8') as f:
        sets = json.load(f)

    logging.info(f"Loaded {len(sets)} total LEGO sets")

    logging.info("Filtering sets...")
    filtered = filter_quality_sets(sets)
    logging.info(f"Final dataset: {len(filtered)} sets")

    logging.info("Creating compact JSON...")
    create_compact_json(filtered)

    themes, parent_themes = extract_themes(filtered)

    logging.info("Creating options.yml...")
    await create_options_yml(filtered, themes, parent_themes)


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