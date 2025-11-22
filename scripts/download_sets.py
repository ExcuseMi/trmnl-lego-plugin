#!/usr/bin/env python3
"""
Download and convert Rebrickable data from CSV to JSON and CSV,
including themes, and attach theme names to sets and minifigs.
"""
import json
import csv
import zipfile
import os
import re
from pathlib import Path
from urllib.request import urlretrieve

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
        'numeric_fields': ['num_parts', 'num_minifigs', 'theme_id']  # theme_id added
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


def download_zip(url, temp_file):
    print(f"Downloading from {url}...")
    urlretrieve(url, temp_file)
    print(f"✓ Downloaded to {temp_file}")


def extract_and_convert(temp_zip, dataset_name, sort_key, numeric_fields):
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
                        row[field] = int(row[field]) if row[field].isdigit() else None
                data.append(row)

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
    with open(out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="||")
        writer.writeheader()
        writer.writerows(data)
    print(f"✓ Saved TXT to {out}")


def cleanup(temp_file):
    if temp_file.exists():
        temp_file.unlink()
        print("✓ Cleaned up temporary file")


def add_theme_names(data, themes_lookup):
    """Attach theme name using theme_id."""
    for item in data:
        tid = item.get("theme_id")
        item["theme"] = themes_lookup.get(tid) if isinstance(tid, int) else None
    return data


def main():
    try:
        print("=== Rebrickable Data Updater ===\n")
        ensure_data_dir()

        # Step 1: Load themes first
        temp_zip = PROJECT_ROOT / "temp_themes.zip"
        download_zip(DATASETS["themes"]["url"], temp_zip)
        themes_data, _ = extract_and_convert(temp_zip, "themes", "id", DATASETS["themes"]["numeric_fields"])
        cleanup(temp_zip)

        themes_lookup = {t["id"]: t.get("name") for t in themes_data}
        print(f"✓ Loaded {len(themes_lookup)} themes")

        # Step 2: Process sets and minifigs with theme names
        for dataset_name in ("sets", "minifigs"):
            config = DATASETS[dataset_name]
            temp_zip = PROJECT_ROOT / f"temp_{dataset_name}.zip"

            try:
                download_zip(config['url'], temp_zip)
                data, fields = extract_and_convert(temp_zip, dataset_name, config['sort_key'], config['numeric_fields'])
                cleanup(temp_zip)

                # Add theme names
                data = add_theme_names(data, themes_lookup)
                if "theme" not in fields:
                    fields.append("theme")

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
