#!/usr/bin/env python3
"""
Download and convert Rebrickable data from CSV to JSON and TXT,
including themes, and attach theme names and parent theme names to sets and minifigs.
"""
import json
import csv
import zipfile
import os
import re
from pathlib import Path
from urllib.request import urlretrieve
from typing import Dict, List, Optional

# Configuration
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
        'numeric_fields': ['num_parts', 'num_minifigs', 'theme_id']
    }
}

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"


def natural_sort_key(value):
    """Natural sorting for LEGO set/fig numbers."""
    def convert(text):
        return (0, int(text)) if text.isdigit() else (1, text.lower())
    parts = re.split(r'(\d+)', str(value))
    return [convert(p) for p in parts if p]


def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✓ Data directory ready: {DATA_DIR}")


def download_zip(url, temp_file: Path):
    print(f"Downloading from {url}...")
    urlretrieve(url, temp_file)
    print(f"✓ Downloaded to {temp_file}")


def extract_and_convert(temp_zip: Path, dataset_name: str, sort_key: str, numeric_fields: List[str]):
    """Extract CSV, normalize `||` line breaks, convert rows, sort."""
    print(f"Extracting and processing {dataset_name} CSV...")

    with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
        csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError("No CSV found in ZIP")
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
                        # attempt integer conversion; non-digit values -> None
                        row[field] = int(row[field]) if row[field].isdigit() else None
                data.append(row)

            # Use stable sort: by year if present, then natural sort of sort_key
            data.sort(key=lambda x: (
                x.get("year") if isinstance(x.get("year"), int) else float("inf"),
                natural_sort_key(x.get(sort_key, ""))
            ))

            print(f"✓ Converted and sorted {len(data)} {dataset_name}")

    return data, csv_reader.fieldnames


def save_json(data, filename):
    out = DATA_DIR / filename
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved JSON to {out}")


def save_txt(data, fieldnames, filename):
    out = DATA_DIR / filename
    # Ensure fieldnames are all strings and in a deterministic order
    fieldnames = list(fieldnames)
    with open(out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="||")
        writer.writeheader()
        writer.writerows(data)
    print(f"✓ Saved TXT to {out}")


def cleanup(temp_file: Path):
    if temp_file.exists():
        temp_file.unlink()
        print("✓ Cleaned up temporary file")


def add_theme_names(
    data: List[Dict],
    themes_lookup: Dict[int, str],
    themes_parent_lookup: Dict[int, Optional[int]],
    max_parent_levels: int = 1
) -> List[Dict]:
    """
    Attach theme name and parent theme name using theme_id.

    - themes_lookup: { theme_id: theme_name }
    - themes_parent_lookup: { theme_id: parent_id_or_None }
    - max_parent_levels: how many steps up the parent chain to include (1 = immediate parent)
    """
    for item in data:
        tid = item.get("theme_id")
        # Normalize tid to int when possible
        if isinstance(tid, str) and tid.isdigit():
            tid = int(tid)
        item["theme"] = themes_lookup.get(tid) if isinstance(tid, int) else None

        # Resolve immediate parent and optionally climb more levels
        parent_name = None
        if isinstance(tid, int):
            parent_id = themes_parent_lookup.get(tid)
            levels = 0
            while parent_id and levels < max_parent_levels:
                # parent_id might be stored as str or int; normalize
                if isinstance(parent_id, str) and parent_id.isdigit():
                    parent_id = int(parent_id)
                parent_name = themes_lookup.get(parent_id)
                # climb
                parent_id = themes_parent_lookup.get(parent_id) if parent_id is not None else None
                levels += 1

        item["parent_theme"] = parent_name

    return data


def main():
    try:
        print("=== Rebrickable Data Updater ===\n")
        ensure_data_dir()

        # Step 1: Load themes first
        temp_zip = PROJECT_ROOT / "temp_themes.zip"
        download_zip(DATASETS["themes"]["url"], temp_zip)
        themes_data, themes_fields = extract_and_convert(temp_zip, "themes", "id", DATASETS["themes"]["numeric_fields"])
        cleanup(temp_zip)

        # Build lookups; ensure keys are ints when possible
        themes_lookup = {}
        themes_parent_lookup = {}
        for t in themes_data:
            tid = t.get("id")
            if isinstance(tid, str) and tid.isdigit():
                tid = int(tid)
            name = t.get("name")
            parent_id = t.get("parent_id")
            if isinstance(parent_id, str) and parent_id.isdigit():
                parent_id = int(parent_id)
            themes_lookup[tid] = name
            themes_parent_lookup[tid] = parent_id

        print(f"✓ Loaded {len([k for k in themes_lookup.keys() if k is not None])} themes")

        # Step 2: Process sets and minifigs with theme names
        for dataset_name in ("sets", "minifigs"):
            config = DATASETS[dataset_name]
            temp_zip = PROJECT_ROOT / f"temp_{dataset_name}.zip"

            try:
                download_zip(config['url'], temp_zip)
                data, fields = extract_and_convert(temp_zip, dataset_name, config['sort_key'], config['numeric_fields'])
                cleanup(temp_zip)

                # Add theme names and parent theme names
                data = add_theme_names(data, themes_lookup, themes_parent_lookup, max_parent_levels=1)

                # Ensure fields include theme + parent_theme
                if "theme" not in fields:
                    fields.append("theme")
                if "parent_theme" not in fields:
                    fields.append("parent_theme")

                save_json(data, f"{dataset_name}.json")
                save_txt(data, fields, f"{dataset_name}.txt")

            except Exception as e:
                print(f"✗ Error processing {dataset_name}: {e}")
                cleanup(temp_zip)
                raise

        print("\n✓ Success! All datasets processed.")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise


if __name__ == "__main__":
    main()
