import json
from pathlib import Path
import yaml

# ==== Paths ====
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"

SETS_FILE = DATA_DIR / "sets.json"
COMPACT_JSON = DATA_DIR / "reduced_sets.json"   # final compact dataset
OUTPUT_FILE = DATA_DIR / "options.yml"


# ==== Load Data ====
def load_sets():
    if not SETS_FILE.exists():
        print(f"Error: {SETS_FILE} not found")
        return None

    with open(SETS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


# ==== Filtering ====
def filter_quality_sets(sets, target_count=8000):
    excluded_themes = {
        'Service Packs', 'Promotional', 'Seasonal', 'Books', 'Gear',
        'Key Chain', 'Magnets', 'Pins', 'Stickers', 'Card Holder'
    }
    excluded_parent_themes = {'Promotional', 'Gear', 'Books'}

    filtered = [
        s for s in sets
        if s.get('image')
        and s.get('num_parts', 0) >= 20
        and s.get('year', 0) >= 1970
        and s.get('theme') not in excluded_themes
        and s.get('parent_theme') not in excluded_parent_themes
    ]

    print(f"  After initial filtering: {len(filtered)} sets")

    if len(filtered) > target_count:
        def score(s):
            score = min(s.get("num_parts", 0), 2000) / 10

            year = s.get("year", 1970)
            if year >= 2020: score += 500
            elif year >= 2010: score += 300
            elif year >= 2000: score += 150
            elif year >= 1990: score += 50

            popular = {
                'Star Wars', 'City', 'Creator', 'Technic', 'Friends',
                'Ninjago', 'Harry Potter', 'Marvel', 'DC', 'Architecture',
                'Ideas', 'Castle', 'Space', 'Pirates', 'Trains'
            }
            if s.get("theme") in popular or s.get("parent_theme") in popular:
                score += 200

            return score

        filtered = sorted(filtered, key=score, reverse=True)[:target_count]
        print(f"  Reduced to top {target_count} sets")

    return filtered


# ==== Write Compact JSON ====
def create_compact_json(sets):
    fields = ["set_num", "name", "year", "num_parts", "image", "theme", "parent_theme"]
    compact = [fields] + [
        [
            s.get("set_num", ""), s.get("name", ""), s.get("year", ""),
            s.get("num_parts", ""), s.get("image", ""),
            s.get("theme", ""), s.get("parent_theme", "")
        ]
        for s in sets
    ]

    with open(COMPACT_JSON, "w", encoding="utf-8") as f:
        json.dump(compact, f, separators=(",", ":"), ensure_ascii=False)

    size_mb = COMPACT_JSON.stat().st_size / (1024 * 1024)
    print(f"  Created {COMPACT_JSON.name}: {size_mb:.2f} MB")


# ==== Extract Theme Lists ====
def extract_themes(sets):
    return (
        sorted({s.get("theme") for s in sets if s.get("theme")}),
        sorted({s.get("parent_theme") for s in sets if s.get("parent_theme")})
    )


# ==== Create YAML ====
def create_options_yml(filtered_sets, themes, parent_themes):
    yaml.add_representer(dict, lambda dumper, data: dumper.represent_mapping("tag:yaml.org,2002:map", data.items()))

    about_field = {
        'keyname': 'about',
        'name': 'About This Plugin',
        'field_type': 'author_bio',
        'description':
            f"Display LEGO sets on your TRMNL device with filtering options.<br /><br />"
            f"<strong>Dataset:</strong><br />"
            f"● {len(filtered_sets):,} curated LEGO sets from Rebrickable<br />"
            f"● Non-LEGO items, micro sets, and sets without images excluded<br />",
        'github_url': 'https://github.com/ExcuseMi/trmnl-lego-plugin'
    }

    fields = [
        about_field,
        {
            'keyname': 'display_order',
            'name': 'Display Order',
            'field_type': 'select',
            'description': 'Choose the order in which sets appear.',
            'options': [
                {'Random': 'random'},
                {'Oldest to Newest': 'incremental'},
                {'Newest to Oldest': 'reverse_incremental'}
            ],
            'default': 'random',
            'optional': True
        },
        {
            'keyname': 'parent_themes',
            'field_type': 'select',
            'name': f'Filter by Parent Themes ({len(parent_themes)})',
            'multiple': True,
            'options': [{p: p} for p in parent_themes],
            'optional': True
        },
        {
            'keyname': 'themes',
            'field_type': 'select',
            'name': f'Filter by Themes ({len(themes)})',
            'multiple': True,
            'options': [{t: t} for t in themes],
            'optional': True
        },
        {
            'keyname': 'min_year',
            'field_type': 'number',
            'name': 'Minimum Release Year',
            'optional': True
        },
        {
            'keyname': 'max_year',
            'field_type': 'number',
            'name': 'Maximum Release Year',
            'optional': True
        }
    ]

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        yaml.dump(fields, f, allow_unicode=True, sort_keys=False, width=1000)

    print(f"  ✓ Created {OUTPUT_FILE.name}")


# ==== Main ====
def main():
    print("=" * 60)
    print(" LEGO Plugin Options Generator")
    print("=" * 60)

    sets = load_sets()
    if not sets:
        return

    print(f"  Loaded {len(sets)} total LEGO sets")

    print("\nFiltering sets...")
    filtered = filter_quality_sets(sets)
    print(f"  Final dataset: {len(filtered)} sets")

    print("\nCreating compact JSON...")
    create_compact_json(filtered)

    themes, parent_themes = extract_themes(filtered)

    print("\nCreating options.yml...")
    create_options_yml(filtered, themes, parent_themes)

    print("\nDone! 🎉")


if __name__ == "__main__":
    main()
