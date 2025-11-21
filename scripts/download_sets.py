#!/usr/bin/env python3
"""
Download and convert Rebrickable sets data from CSV to JSON.
"""
import json
import csv
import zipfile
import os
from pathlib import Path
from urllib.request import urlretrieve

# Configuration
CSV_URL = "https://cdn.rebrickable.com/media/downloads/sets.csv.zip"
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_FILE = DATA_DIR / "sets.json"
TEMP_ZIP = PROJECT_ROOT / "temp_sets.zip"


def ensure_data_dir():
    """Create data directory if it doesn't exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✓ Data directory ready: {DATA_DIR}")


def download_zip():
    """Download the CSV zip file."""
    print(f"Downloading from {CSV_URL}...")
    urlretrieve(CSV_URL, TEMP_ZIP)
    print(f"✓ Downloaded to {TEMP_ZIP}")


def extract_and_convert():
    """Extract CSV from zip and convert to JSON."""
    print("Extracting and converting CSV to JSON...")

    with zipfile.ZipFile(TEMP_ZIP, 'r') as zip_ref:
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
            sets_data = []
            for row in csv_reader:
                # Convert numeric fields
                if row['year']:
                    row['year'] = int(row['year']) if row['year'].isdigit() else None
                if row['theme_id']:
                    row['theme_id'] = int(row['theme_id']) if row['theme_id'].isdigit() else None
                if row['num_parts']:
                    row['num_parts'] = int(row['num_parts']) if row['num_parts'].isdigit() else None

                sets_data.append(row)

            print(f"✓ Converted {len(sets_data)} sets")

    return sets_data


def save_json(data):
    """Save data to JSON file."""
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=0, ensure_ascii=False)
    print(f"✓ Saved to {OUTPUT_FILE}")


def cleanup():
    """Remove temporary zip file."""
    if TEMP_ZIP.exists():
        TEMP_ZIP.unlink()
        print(f"✓ Cleaned up temporary file")


def main():
    """Main execution function."""
    try:
        print("=== Rebrickable Sets Data Updater ===\n")

        ensure_data_dir()
        download_zip()
        sets_data = extract_and_convert()
        save_json(sets_data)
        cleanup()

        print(f"\n✓ Success! JSON file created with {len(sets_data)} sets")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        cleanup()
        raise


if __name__ == "__main__":
    main()