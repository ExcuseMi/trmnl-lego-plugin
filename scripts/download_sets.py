#!/usr/bin/env python3
"""
Download and convert Rebrickable data from CSV to JSON and CSV.
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
    'sets': {
        'url': 'https://cdn.rebrickable.com/media/downloads/sets.csv.zip',
        'sort_key': 'set_num',
        'numeric_fields': ['year', 'theme_id', 'num_parts']
    },
    'minifigs': {
        'url': 'https://cdn.rebrickable.com/media/downloads/minifigs.csv.zip',
        'sort_key': 'fig_num',
        'numeric_fields': ['num_parts']
    }
}

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"


def natural_sort_key(value):
    """
    Create a sort key that handles LEGO set numbers naturally.
    Always return consistent tuple structure to avoid mixed-type comparison.
    Example keys: "001-1", "10-1", "100-A1"
    """

    def convert(text):
        if text.isdigit():
            # Numeric portion: tuple (0, number)
            return (0, int(text))
        # Text portion: tuple (1, lowercase-string)
        return (1, text.lower())

    parts = re.split(r'(\d+)', str(value))
    return [convert(part) for part in parts if part]


def ensure_data_dir():
    """Create data directory if it doesn't exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✓ Data directory ready: {DATA_DIR}")


def download_zip(url, temp_file):
    """Download the CSV zip file."""
    print(f"Downloading from {url}...")
    urlretrieve(url, temp_file)
    print(f"✓ Downloaded to {temp_file}")


def extract_and_convert(temp_zip, dataset_name, sort_key, numeric_fields):
    """Extract CSV from zip and convert to sorted list."""
    print(f"Extracting and processing {dataset_name} CSV...")

    with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
        # Find the CSV file in the zip
        csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]

        if not csv_files:
            raise FileNotFoundError("No CSV file found in the zip archive")

        csv_filename = csv_files[0]
        print(f"✓ Found CSV file: {csv_filename}")

        # Read CSV directly from zip
        with zip_ref.open(csv_filename) as csv_file:
            # Decode bytes to string
            csv_text = csv_file.read().decode('utf-8')
            csv_reader = csv.DictReader(csv_text.splitlines())

            # Convert to list of dictionaries
            data = []
            for row in csv_reader:
                # Convert numeric fields
                for field in numeric_fields:
                    if field in row and row[field]:
                        row[field] = int(row[field]) if row[field].isdigit() else None

                data.append(row)

            # Sort by the sort_key using natural sorting
            data.sort(
                key=lambda x: (
                    x.get("year") if isinstance(x.get("year"), int) else float("inf"),
                    natural_sort_key(x.get(sort_key, ""))
                )
            )
            print(f"✓ Converted and sorted {len(data)} {dataset_name}")

    return data, csv_reader.fieldnames


def save_json(data, filename):
    """Save data to JSON file."""
    output_file = DATA_DIR / filename
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved JSON to {output_file}")


def save_csv(data, fieldnames, filename):
    """Save data to CSV file."""
    output_file = DATA_DIR / filename
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"✓ Saved CSV to {output_file}")


def cleanup(temp_file):
    """Remove temporary zip file."""
    if temp_file.exists():
        temp_file.unlink()
        print(f"✓ Cleaned up temporary file")


def process_dataset(dataset_name, config):
    """Process a single dataset."""
    print(f"\n--- Processing {dataset_name} ---")

    temp_zip = PROJECT_ROOT / f"temp_{dataset_name}.zip"

    try:
        download_zip(config['url'], temp_zip)
        data, fieldnames = extract_and_convert(
            temp_zip,
            dataset_name,
            config['sort_key'],
            config['numeric_fields']
        )
        save_json(data, f"{dataset_name}.json")
        save_csv(data, fieldnames, f"{dataset_name}.csv")
        cleanup(temp_zip)

        return len(data)

    except Exception as e:
        print(f"✗ Error processing {dataset_name}: {e}")
        cleanup(temp_zip)
        raise


def main():
    """Main execution function."""
    try:
        print("=== Rebrickable Data Updater ===\n")

        ensure_data_dir()

        total_records = 0
        for dataset_name, config in DATASETS.items():
            count = process_dataset(dataset_name, config)
            total_records += count

        print(f"\n✓ Success! Processed {total_records} total records across {len(DATASETS)} datasets")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise


if __name__ == "__main__":
    main()