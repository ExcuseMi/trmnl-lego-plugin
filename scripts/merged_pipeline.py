#!/usr/bin/env python3
"""
Complete LEGO data pipeline: Download Rebrickable data, filter and validate,
then generate options.yml and theme-based compact JSON files for TRMNL plugin.

- Downloads Rebrickable CSV zips for themes, sets and minifigs.
- Normalizes and validates images (caching HEAD results).
- Saves full JSON datasets (sets.json, minifigs.json, themes.json) under data/.
- Creates compact per-theme files stored at data/themes/{theme-slug}/0.json
  (each file < MAX_FILE_SIZE_KB).
- Produces options.yml where each theme option is:
    { "Theme Name : {set_count}": "{theme_slug}|{max_file_count}" }
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
PROJECT_ROOT = SCRIPT_DIR.parent if SCRIPT_DIR.parent.exists() else SCRIPT_DIR
DATA_DIR = PROJECT_ROOT / "data"
THEME_DIR = DATA_DIR / "themes"

SETS_FILE = DATA_DIR / "sets.json"
MINIFIGS_FILE = DATA_DIR / "minifigs.json"
THEMES_FILE = DATA_DIR / "themes.json"
OUTPUT_FILE = DATA_DIR / "options.yml"

# Fields used in compact per-theme JSON files (order matters)
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
    """Fetch all themes from BrickSet API (optional, requires BRICKSET_API_KEY)."""
    if not BRICKSET_API_KEY:
        logging.warning("No BRICKSET_API_KEY found in .env, skipping BrickSet theme fetch")
        return []

    url = f"https://brickset.com/api/v3.asmx/getThemes?apikey={BRICKSET_API_KEY}"

    try:
        headers = {'Accept': 'application/json'}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    text = await response.text()
                    # Brickset sometimes returns XML — we expect JSON from your original code,
                    # but guard against JSON decode errors.
                    try:
                        data = json.loads(text)
                        themes = data.get('themes', [])
                        theme_names = sorted([t['theme'] for t in themes if 'theme' in t])
                        logging.info(f"Fetched {len(theme_names)} themes from BrickSet")
                        return theme_names
                    except Exception:
                        logging.error("BrickSet response not JSON or unexpected format")
                        return []
                else:
                    logging.error(f"Failed to fetch BrickSet themes: HTTP {response.status}")
                    return []
    except Exception as e:
        logging.error(f"Error fetching BrickSet themes: {e}")
        return []


# ==== Utility Functions ====
def slugify(text: str) -> str:
    """Convert text to URL-safe slug"""
    if text is None:
        return "unknown"
    text = str(text)
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text.strip('-') or "unknown"


def natural_sort_key(value):
    def convert(text):
        return (0, int(text)) if text.isdigit() else (1, text.lower())
    parts = re.split(r'(\d+)', str(value))
    return [convert(p) for p in parts if p]


def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    THEME_DIR.mkdir(parents=True, exist_ok=True)
    logging.info(f"Data directories ready: {DATA_DIR}, {THEME_DIR}")


def download_zip(url, temp_file: Path):
    logging.info(f"Downloading from {url}...")
    urlretrieve(url, temp_file)
    logging.info(f"Downloaded to {temp_file}")


def extract_and_convert(temp_zip: Path, dataset_name: str, sort_key: str, numeric_fields: list):
    """
    Extract the first CSV from the supplied ZIP and convert to list of dicts.
    Numeric fields that look like ints will be converted.
    Returns (data_list, fieldnames)
    """
    logging.info(f"Extracting and processing {dataset_name} CSV from {temp_zip}...")
    with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
        csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError(f"No CSV found in ZIP for {dataset_name}")
        csv_filename = csv_files[0]
        logging.info(f"Found CSV file in zip: {csv_filename}")

        with zip_ref.open(csv_filename) as csv_file:
            csv_text = csv_file.read().decode("utf-8", errors="replace")
            # Fix common format oddities
            csv_text = csv_text.replace("||", "\n").replace("\r\n", "\n").replace("\r", "\n")
            csv_reader = csv.DictReader(csv_text.splitlines())
            data = []
            for row in csv_reader:
                # Convert numeric-ish fields
                for field in numeric_fields:
                    if field in row and row[field] != "":
                        v = row[field].strip()
                        if v.isdigit():
                            row[field] = int(v)
                        else:
                            # try int conversion ignoring non-digits
                            try:
                                row[field] = int(re.sub(r'\D', '', v))
                            except Exception:
                                row[field] = None
                    else:
                        row[field] = None
                # Keep original row (including extra fields) for possible future use.
                data.append(row)
    logging.info(f"Extracted {len(data)} rows for {dataset_name}")
    return data, csv_reader.fieldnames


def add_theme_names(data, themes_lookup, parent_lookup):
    """
    Add 'theme' and 'parent_theme' textual fields to the data rows based on theme_id.
    We only set them when theme_id is an int and a matching theme exists.
    """
    for item in data:
        tid = item.get("theme_id")
        # handle both int and numeric string
        if isinstance(tid, str) and tid.isdigit():
            tid = int(tid)
        if isinstance(tid, int):
            item["theme"] = themes_lookup.get(tid, "")
            # parent_lookup maps theme_id -> parent_name (maybe empty)
            item["parent_theme"] = parent_lookup.get(tid, "") or ""
        else:
            item["theme"] = item.get("theme", "") or ""
            item["parent_theme"] = item.get("parent_theme", "") or ""
    return data


def save_json(data, filename):
    out = DATA_DIR / filename
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logging.info(f"Saved JSON to {out}")


def cleanup(temp_file: Path):
    if temp_file.exists():
        try:
            temp_file.unlink()
            logging.info(f"Cleaned up temporary file {temp_file}")
        except Exception as e:
            logging.warning(f"Failed to remove temp file {temp_file}: {e}")


# ==== Async Image Validation ====
async def check_image(session, url, semaphore):
    """HEAD the URL to check if image exists (status 200)."""
    async with semaphore:
        try:
            # Use HEAD first — some servers may not like it, fall back to GET if needed
            async with session.head(url, timeout=10) as resp:
                if resp.status == 200:
                    return url, True
                # Some CDNs reject HEAD; try GET (without downloading body)
                if resp.status in (403, 405):
                    async with session.get(url, timeout=10) as resp2:
                        return url, resp2.status == 200
                return url, False
        except Exception:
            # Last resort: try GET
            try:
                async with session.get(url, timeout=10) as resp:
                    return url, resp.status == 200
            except Exception:
                return url, False


async def validate_images(data):
    """
    Validate images and filter out rows with missing or invalid images.
    Only stores set_num in the data for downstream compact files but preserves original rows in sets.json.
    Uses a cached image_cache.json to avoid repeated HEAD requests.
    """
    cache_file = DATA_DIR / "image_cache.json"
    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            logging.info(f"Loaded image cache with {len(cache)} entries")
        except Exception:
            cache = {}
    else:
        cache = {}

    # Build URLs to check
    urls_to_check = []
    set_num_to_url = {}
    for row in data:
        set_num = row.get("set_num") or row.get("fig_num") or ""
        if set_num:
            # normalize set number string for url (Rebrickable uses set_num like '0001-1' with slash? keep raw)
            url = f"https://cdn.rebrickable.com/media/sets/{set_num}.jpg"
            set_num_to_url[set_num] = url
            if url not in cache:
                urls_to_check.append(url)

    logging.info(f"Checking {len(urls_to_check)} new images...")

    if urls_to_check:
        semaphore = asyncio.Semaphore(20)
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [check_image(session, url, semaphore) for url in urls_to_check]
            checked = 0
            for coro in asyncio.as_completed(tasks):
                url, valid = await coro
                cache[url] = bool(valid)
                checked += 1
                if checked % 100 == 0:
                    logging.info(f"Progress: {checked}/{len(urls_to_check)} images checked")

    # Filter data - only keep rows with valid images
    filtered_data = []
    removed_count = 0
    for row in data:
        set_num = row.get("set_num") or row.get("fig_num") or ""
        if set_num:
            url = set_num_to_url.get(set_num)
            if url and cache.get(url, False):
                # Remove any 'image' field to avoid storing large URLs; downstream compact will only store set_num
                row.pop("image", None)
                filtered_data.append(row)
            else:
                removed_count += 1
        else:
            removed_count += 1

    # Save cache
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        logging.warning(f"Failed to save image cache: {e}")

    logging.info(f"Image validation complete. Retained: {len(filtered_data)} Removed: {removed_count}")
    return filtered_data


# ==== Data Download & Processing ====
async def download_and_process_rebrickable():
    """Download and process Rebrickable CSV datasets: themes, sets, minifigs."""
    logging.info("=== Phase 1: Download & Process Rebrickable Data ===")

    # === Themes ===
    temp_zip = PROJECT_ROOT / "temp_themes.zip"
    download_zip(DATASETS["themes"]["url"], temp_zip)
    themes_data, _ = extract_and_convert(temp_zip, "themes", DATASETS["themes"]["sort_key"],
                                         DATASETS["themes"]["numeric_fields"])
    cleanup(temp_zip)

    # Build lookups for theme id -> name and parent id -> parent name
    themes_lookup = {}
    for t in themes_data:
        # theme CSV may have 'id' and 'name' or different keys; handle robustly
        tid = t.get("id")
        name = t.get("name") or t.get("theme") or t.get("title") or ""
        if isinstance(tid, str) and tid.isdigit():
            try:
                tid = int(tid)
            except Exception:
                pass
        themes_lookup[tid] = name

    parent_lookup = {}
    for t in themes_data:
        tid = t.get("id")
        parent_id = t.get("parent_id")
        if isinstance(tid, str) and tid.isdigit():
            try:
                tid = int(tid)
            except Exception:
                pass
        if isinstance(parent_id, str) and parent_id.isdigit():
            try:
                parent_id = int(parent_id)
            except Exception:
                parent_id = None
        parent_name = themes_lookup.get(parent_id, "") if parent_id else ""
        parent_lookup[tid] = parent_name

    logging.info(f"Loaded {len(themes_lookup)} themes")

    # Save themes full data for reference
    save_json(themes_data, "themes.json")

    # === Sets and Minifigs ===
    for dataset_name in ("sets", "minifigs"):
        config = DATASETS[dataset_name]
        temp_zip = PROJECT_ROOT / f"temp_{dataset_name}.zip"
        logging.info(f"Processing dataset: {dataset_name}")

        download_zip(config['url'], temp_zip)
        data, fieldnames = extract_and_convert(temp_zip, dataset_name, config['sort_key'], config['numeric_fields'])
        cleanup(temp_zip)

        # Add theme display names
        data = add_theme_names(data, themes_lookup, parent_lookup)

        # Normalize rows to preserve many original fields but guarantee some keys exist
        normalized_data = []
        for row in data:
            # Keep all original fields but add/normalize our standard ones
            normalized_row = dict(row)  # shallow copy of original
            # ensure keys exist
            normalized_row.setdefault("set_num", row.get("set_num") or row.get("fig_num") or "")
            normalized_row.setdefault("name", row.get("name") or "")
            normalized_row.setdefault("year", row.get("year") if row.get("year") is not None else "")
            normalized_row.setdefault("num_parts", row.get("num_parts") if row.get("num_parts") is not None else "")
            normalized_row.setdefault("theme", row.get("theme") or "")
            normalized_row.setdefault("parent_theme", row.get("parent_theme") or "")
            normalized_data.append(normalized_row)

        # Sorting: sets by year then natural set_num; minifigs by fig_num natural order
        sort_key = "set_num" if dataset_name == "sets" else "fig_num"
        year_key = "year" if dataset_name == "sets" else None

        def sort_key_fn(x):
            primary = x.get(year_key) if year_key and isinstance(x.get(year_key), int) else float("inf")
            secondary = natural_sort_key(x.get(sort_key, ""))
            return (primary, secondary)

        normalized_data.sort(key=sort_key_fn)

        # Validate images (this trims rows without valid set image)
        validated = await validate_images(normalized_data)

        logging.info(f"{dataset_name}: {len(validated)} rows after validation")

        # Save full dataset (validated rows) for future usage
        if dataset_name == "sets":
            save_json(validated, "sets.json")
        else:
            save_json(validated, "minifigs.json")


# ==== Theme-Based File Creation ====
def create_theme_files(sets):
    """
    Create compact per-theme JSON files under:
        data/themes/{theme-slug}/0.json
    Each file will be trimmed so its UTF-8 JSON size is <= MAX_FILE_SIZE_KB.
    Returns a theme_info dict mapping slug -> {"name":..., "count":..., "max_file_count": ...}
    (max_file_count is 1 currently since we only create 0.json; reserved for future pagination)
    """
    # Group sets by theme name (use 'Unknown' if missing)
    theme_sets = {}
    for s in sets:
        theme_name = s.get("theme") or "Unknown"
        theme_sets.setdefault(theme_name, []).append(s)

    logging.info(f"Found {len(theme_sets)} unique themes")

    theme_info = {}

    for theme_name, theme_set_list in theme_sets.items():
        slug = slugify(theme_name)
        theme_path = THEME_DIR / slug
        theme_path.mkdir(parents=True, exist_ok=True)
        output_file = theme_path / "0.json"

        # Use the agreed compact fields order
        fields = list(FIELDS_ORDER)

        # Sort by year and natural set number
        theme_set_list.sort(key=lambda x: (
            x.get("year") if isinstance(x.get("year"), int) else float("inf"),
            natural_sort_key(x.get("set_num", ""))
        ))

        # Build the compact structure (fields + rows)
        compact = [fields] + [
            [
                s.get("set_num", "") or "",
                s.get("name", "") or "",
                s.get("year", "") or "",
                s.get("num_parts", "") or "",
                s.get("theme", "") or "",
                s.get("parent_theme", "") or ""
            ]
            for s in theme_set_list
        ]

        # Check size and trim via binary search if necessary
        json_str = json.dumps(compact, separators=(",", ":"), ensure_ascii=False)
        size_kb = len(json_str.encode('utf-8')) / 1024

        if size_kb > MAX_FILE_SIZE_KB:
            logging.warning(f"Theme '{theme_name}' ({len(theme_set_list)} sets) exceeds {MAX_FILE_SIZE_KB}KB ({size_kb:.2f}KB)")
            left, right = 1, len(theme_set_list)
            max_sets = 1
            while left <= right:
                mid = (left + right) // 2
                test_compact = [fields] + [
                    [
                        s.get("set_num", "") or "",
                        s.get("name", "") or "",
                        s.get("year", "") or "",
                        s.get("num_parts", "") or "",
                        s.get("theme", "") or "",
                        s.get("parent_theme", "") or ""
                    ]
                    for s in theme_set_list[:mid]
                ]
                test_size_kb = len(json.dumps(test_compact, separators=(",", ":"), ensure_ascii=False).encode('utf-8')) / 1024
                if test_size_kb <= MAX_FILE_SIZE_KB:
                    max_sets = mid
                    left = mid + 1
                else:
                    right = mid - 1

            # Trim the list to the determined maximum
            theme_set_list = theme_set_list[:max_sets]
            compact = [fields] + [
                [
                    s.get("set_num", "") or "",
                    s.get("name", "") or "",
                    s.get("year", "") or "",
                    s.get("num_parts", "") or "",
                    s.get("theme", "") or "",
                    s.get("parent_theme", "") or ""
                ]
                for s in theme_set_list
            ]
            json_str = json.dumps(compact, separators=(",", ":"), ensure_ascii=False)
            size_kb = len(json_str.encode('utf-8')) / 1024
            logging.info(f"  Trimmed to {len(theme_set_list)} sets ({size_kb:.2f}KB)")

        # Write compact JSON to file
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(json_str)
            logging.info(f"Created {output_file.relative_to(PROJECT_ROOT)}: {len(theme_set_list)} sets, {size_kb:.2f}KB")
        except Exception as e:
            logging.error(f"Failed to write {output_file}: {e}")

        # Record theme info; max_file_count remains 1 (we create only one file per theme currently)
        theme_info[slug] = {
            "name": theme_name,
            "count": len(theme_set_list),
            "max_file_count": 1
        }

    return theme_info

def save_theme_option_files(theme_info, brickset_themes):
    """Save theme options for both vendors into two JSON files"""

    # ===== Rebrickable themes =====
    rebrickable_options = [
        { info["name"] : f"{slug}|{info['max_file_count']}" }
        for slug, info in sorted(theme_info.items(), key=lambda x: x[1]["name"])
    ]

    with open(DATA_DIR / "rebrickable_themes.json", "w", encoding="utf-8") as f:
        json.dump(rebrickable_options, f, ensure_ascii=False, indent=2)

    logging.info(f"✓ Saved rebrickable themes → data/rebrickable_themes.json")

    # ===== Brickset themes =====
    brickset_options = [
        { name : name } for name in sorted(brickset_themes)
    ]

    with open(DATA_DIR / "brickset_themes.json", "w", encoding="utf-8") as f:
        json.dump(brickset_options, f, ensure_ascii=False, indent=2)

    logging.info(f"✓ Saved brickset themes → data/brickset_themes.json")

# ==== Create YAML (options.yml) ====
async def create_options_yml(theme_info):
    """
    Create options.yml used by TRMNL plugin.
    The Rebrickable theme options look like:
        { "Theme Name : {count}": "theme-slug|max_file_count" }
    """
    # Ensure yaml keeps mapping order and prints nicely
    yaml.add_representer(dict, lambda dumper, data: dumper.represent_mapping("tag:yaml.org,2002:map", data.items()))

    # Try to fetch BrickSet themes if API key is present
    themes_brickset = await fetch_brickset_themes()

    # Sort themes by human-readable name
    sorted_themes = sorted(theme_info.items(), key=lambda x: x[1]["name"].lower())

    # About field (rich description)
    about_field = {
        'keyname': 'about',
        'name': 'About This Plugin',
        'field_type': 'author_bio',
        'description':
            f"Display LEGO® sets on your TRMNL device with filtering options using live data from community APIs.<br /><br />"
            f"<strong>Data Sources:</strong><br />"
            f"● <strong>Rebrickable Mode (default):</strong> Uses cached data from <a href='https://rebrickable.com/'>Rebrickable.com</a> with theme and part filters<br />"
            f"● <strong>BrickSet Mode:</strong> Uses live set data from <a href='https://brickset.com/'>BrickSet.com</a> with optional personal collection features (owned/wanted), minifigs, and regional pricing<br /><br />"
            f"<strong>BrickSet Setup (Optional):</strong><br />"
            f"1. Create a free account at <a href='https://brickset.com/signup'>brickset.com/signup</a><br />"
            f"2. Request your API key at <a href='https://brickset.com/tools/webservices/requestkey'>brickset.com/tools/webservices/requestkey</a><br />"
            f"3. Enter your API key below to enable BrickSet mode<br />"
            f"4. (Optional) For owned/wanted filtering: call the login API to get your userHash (see Brickset API docs)<br /><br />"
            f"<strong>Theme Note:</strong> Both APIs use different theme names and categories. Your filter options will automatically change depending on the selected data source.",
        'github_url': 'https://github.com/ExcuseMi/trmnl-lego-plugin',
        'category': 'life'
    }

    # Build fields list with Rebrickable theme options formatted as requested
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
            'conditional_validation': [
                {
                    "when": "brickset",
                    "required": ["brickset_api_key", "brickset_user_hash", "themes_brickset",
                                 "brickset_owned_wanted", "brickset_pricing", "brickset_show_minifigs_included"],
                    "hidden": ["themes_rebrickable"]
                },
                {
                    "when": "rebrickable",
                    "required": ["themes_rebrickable"],
                    "hidden": ["brickset_api_key", "brickset_user_hash", "themes_brickset",
                               "brickset_owned_wanted", "brickset_pricing", "brickset_show_minifigs_included"]
                }
            ]
        },
        {
            'keyname': 'brickset_api_key',
            'field_type': 'string',
            'name': 'Brickset API Key (Optional)',
            'description': 'Enter your Brickset Web Services API key to enable Brickset mode. Get your key at brickset.com.',
            'placeholder': 'Your API key'
        },
        {
            'keyname': 'brickset_user_hash',
            'field_type': 'string',
            'name': 'Brickset User Hash (Optional)',
            'description': 'Required to display owned/wanted collections. Obtain it via the Brickset API login method.',
            'optional': True,
            'placeholder': 'Your user hash'
        },
        {
            'keyname': 'themes_rebrickable',
            'field_type': 'select',
            'name': f'Filter by Themes – Rebrickable ({len(sorted_themes)})',
            'description': 'Applicable only when using the curated Rebrickable dataset. Select themes to include in the display.',
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
            'description': 'Applicable only in Brickset API mode. Filters sets at the server level using Brickset’s official theme names.',
            'multiple': True,
            'options': [{t: t} for t in themes_brickset],
        },
        {
            'keyname': 'show_qr_code',
            'name': 'Show QR Code Link',
            'field_type': 'select',
            'description': 'Display a QR code linking to the selected set’s details page.',
            'options': [
                {'Hide QR Code': 'hide'},
                {'Show QR Code': 'show'},
            ],
            'default': 'hide',
        },
        {
            'keyname': 'brickset_pricing',
            'field_type': 'select',
            'name': 'Show Set Price (Brickset Only)',
            'description': 'Displays the LEGO® retail price from LEGO.com in the selected region (if available).',
            'options': [
                {'Do Not Show Price': ''},
                {'Canada (CA)': 'CA'},
                {'Germany (DE)': 'DE'},
                {'United Kingdom (UK)': 'UK'},
                {'United States (US)': 'US'},
            ],
            'default': '',
        },
        {
            'keyname': 'brickset_show_minifigs_included',
            'field_type': 'select',
            'name': 'Show Number of Minifigures (Brickset Only)',
            'description': 'Displays the number of minifigures included in the set when Brickset provides the data.',
            'options': [
                {'Yes, Show Minifigs': 'yes'},
                {'No, Do Not Display': 'no'},
            ],
            'default': 'no',
        },
        {
            'keyname': 'min_year',
            'field_type': 'number',
            'name': 'Minimum Release Year',
            'description': 'Filters by release year. This filter occurs locally for curated mode and server-side for Brickset.',
            'min': 1900,
            'optional': True,
            'placeholder': "1950"
        },
        {
            'keyname': 'max_year',
            'field_type': 'number',
            'name': 'Maximum Release Year',
            'description': 'Filters by release year. This filter occurs locally for curated mode and on the API request for Brickset.',
            'min': 1900,
            'optional': True,
            'placeholder': f'{datetime.date.today().year}'
        },
        {
            'keyname': 'brickset_owned_wanted',
            'field_type': 'select',
            'name': 'Filter by Collection Status (Brickset Only)',
            'description': 'Filter Brickset results to only show sets you own or want. Requires a user hash to be provided.',
            'options': [
                {'Show All Sets': ''},
                {'Only Show Owned Sets': '%27owned%27:1,'},
                {'Only Show Wanted Sets': '%27wanted%27:1,'},
            ],
            'default': '',
            'optional': True
        },
        {
            'keyname': 'min_parts',
            'field_type': 'number',
            'name': 'Minimum Number of Parts',
            'description': 'Show only sets containing at least this many pieces. Filter applied locally in both modes.',
            'min': 0,
            'optional': True,
            'placeholder': "100"
        },
        {
            'keyname': 'max_parts',
            'field_type': 'number',
            'name': 'Maximum Number of Parts',
            'description': 'Show only sets containing at most this many pieces. Filter applied locally in both modes.',
            'min': 0,
            'optional': True,
            'placeholder': "5000"
        }
    ]

    # Write YAML
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            yaml.dump(fields, f, allow_unicode=True, sort_keys=False, width=1000)
        logging.info(f"✓ Created {OUTPUT_FILE}")
    except Exception as e:
        logging.error(f"Failed to write {OUTPUT_FILE}: {e}")


# ==== Generate Options (Phase 2) ====
async def generate_options():
    """Generate options.yml and theme-based JSON files from saved sets.json"""
    logging.info("=== Phase 2: Generate Plugin Options ===")

    if not SETS_FILE.exists():
        logging.error(f"Error: {SETS_FILE} not found. Run Phase 1 first (download_and_process_rebrickable).")
        return

    with open(SETS_FILE, 'r', encoding='utf-8') as f:
        sets = json.load(f)

    logging.info(f"Loaded {len(sets)} total LEGO sets from {SETS_FILE}")

    logging.info("Creating theme-based compact files...")
    theme_info = create_theme_files(sets)

    logging.info("Creating options.yml...")
    await create_options_yml(theme_info)
    # Fetch Brickset themes again (only name list needed)
    brickset_themes = await fetch_brickset_themes()

    # Save vendor options into two JSON files
    save_theme_option_files(theme_info, brickset_themes)

# ==== Main ====
async def main():
    try:
        logging.info("=" * 60)
        logging.info(" LEGO Data Pipeline for TRMNL Plugin")
        logging.info("=" * 60)

        ensure_data_dir()

        # Phase 1: Download and process Rebrickable data
        await download_and_process_rebrickable()

        # Phase 2: Generate plugin options and per-theme files
        await generate_options()

        logging.info("\n✓ Success! Pipeline complete. 🎉")

    except Exception as e:
        logging.exception("Error during processing")
        raise


if __name__ == "__main__":
    asyncio.run(main())
