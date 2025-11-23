import json
from pathlib import Path
import yaml

# Get the script's directory and navigate to project root
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
SETS_FILE = DATA_DIR / "sets.json"
REDUCED_SETS_JSON = DATA_DIR / "reduced_sets.json"
REDUCED_SETS_TXT = DATA_DIR / "reduced_sets.txt"
OUTPUT_FILE = DATA_DIR / "options.yml"


def load_sets():
    """Load sets data from JSON file"""
    if not SETS_FILE.exists():
        print(f"Error: {SETS_FILE} not found")
        return None

    with open(SETS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def filter_quality_sets(sets, target_count=8000):
    """
    Filter sets to keep only the most interesting ones.
    Returns approximately target_count sets.
    """
    filtered = []

    # Themes to exclude (promotional, low-interest, non-traditional sets)
    excluded_themes = {
        'Service Packs', 'Promotional', 'Seasonal', 'Books', 'Gear',
        'Key Chain', 'Magnets', 'Pins', 'Stickers', 'Card Holder'
    }

    # Parent themes that are typically lower interest
    excluded_parent_themes = {
        'Promotional', 'Gear', 'Books'
    }

    for set_data in sets:
        theme = set_data.get('theme', '')
        parent_theme = set_data.get('parent_theme', '')
        num_parts = set_data.get('num_parts', 0)
        year = set_data.get('year', 0)

        # Skip if theme is in excluded list
        if theme in excluded_themes or parent_theme in excluded_parent_themes:
            continue

        # Skip sets with too few pieces (less interesting to display)
        if num_parts < 20:
            continue

        # Skip very old sets (before 1970) - mostly incomplete data
        if year < 1970:
            continue

        # Skip sets with no image
        if not set_data.get('image'):
            continue

        filtered.append(set_data)

    print(f"  After initial filtering: {len(filtered)} sets")

    # If still too many, prioritize by piece count and recency
    if len(filtered) > target_count:
        # Score each set based on desirability
        def score_set(s):
            score = 0

            # More pieces = more interesting (up to a point)
            pieces = min(s.get('num_parts', 0), 2000)
            score += pieces / 10

            # More recent sets = more relevant
            year = s.get('year', 1970)
            if year >= 2020:
                score += 500
            elif year >= 2010:
                score += 300
            elif year >= 2000:
                score += 150
            elif year >= 1990:
                score += 50

            # Popular themes get a boost
            popular_themes = {
                'Star Wars', 'City', 'Creator', 'Technic', 'Friends',
                'Ninjago', 'Harry Potter', 'Marvel', 'DC', 'Architecture',
                'Ideas', 'Castle', 'Space', 'Pirates', 'Trains'
            }
            if s.get('parent_theme') in popular_themes or s.get('theme') in popular_themes:
                score += 200

            return score

        # Sort by score and keep top sets
        filtered.sort(key=score_set, reverse=True)
        filtered = filtered[:target_count]
        print(f"  Reduced to top {target_count} sets based on scoring")

    return filtered


def create_data_files(sets):
    """Create both JSON and TXT data files"""
    # Save as JSON
    with open(REDUCED_SETS_JSON, 'w', encoding='utf-8') as f:
        json.dump(sets, f, indent=2, ensure_ascii=False)

    json_size_mb = REDUCED_SETS_JSON.stat().st_size / (1024 * 1024)
    print(f"  Created {REDUCED_SETS_JSON.name}: {json_size_mb:.2f} MB")

    # Save as TXT (pipe-delimited)
    with open(REDUCED_SETS_TXT, 'w', encoding='utf-8') as f:
        for set_data in sets:
            # Format: set_num§name§year§num_parts§image§theme§parent_theme
            line = "§".join([
                str(set_data.get('set_num', '')),
                str(set_data.get('name', '')),
                str(set_data.get('year', '')),
                str(set_data.get('num_parts', '')),
                str(set_data.get('image', '')),
                str(set_data.get('theme', '')),
                str(set_data.get('parent_theme', ''))
            ])
            f.write(line + "||")

    txt_size_mb = REDUCED_SETS_TXT.stat().st_size / (1024 * 1024)
    print(f"  Created {REDUCED_SETS_TXT.name}: {txt_size_mb:.2f} MB")


def extract_themes(sets):
    """Extract unique themes and parent themes from sets data and return sorted lists"""
    themes = set()
    parent_themes = set()

    for set_data in sets:
        theme = set_data.get("theme")
        parent_theme = set_data.get("parent_theme")

        if theme:
            themes.add(theme)
        if parent_theme:
            parent_themes.add(parent_theme)

    # Sort themes alphabetically
    sorted_themes = sorted(themes)
    sorted_parent_themes = sorted(parent_themes)

    return sorted_themes, sorted_parent_themes


def print_dataset_statistics(sets):
    """Print statistics about the dataset"""
    years = [s.get('year', 0) for s in sets if s.get('year', 0) > 0]
    pieces = [s.get('num_parts', 0) for s in sets if s.get('num_parts', 0) > 0]

    print(f"\n  Dataset Statistics:")
    print(f"    Year range: {min(years)} - {max(years)}")
    print(f"    Piece count range: {min(pieces)} - {max(pieces)}")
    print(f"    Average pieces: {sum(pieces) / len(pieces):.0f}")

    # Show theme distribution
    themes = {}
    for s in sets:
        pt = s.get('parent_theme', 'Unknown')
        themes[pt] = themes.get(pt, 0) + 1

    print(f"\n  Top 10 Parent Themes:")
    for theme, count in sorted(themes.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"    {theme}: {count} sets")


def create_options_yml():
    print("=" * 60)
    print("LEGO Plugin Options Generator")
    print("=" * 60)

    # Load data
    print(f"\nLoading sets from {SETS_FILE.name}...")
    all_sets = load_sets()

    if not all_sets:
        print("\nFailed to load required data. Exiting.")
        return

    print(f"  Loaded {len(all_sets)} total LEGO sets")

    # Filter to quality sets
    print(f"\nFiltering to quality sets...")
    filtered_sets = filter_quality_sets(all_sets, target_count=8000)
    print(f"  Final dataset: {len(filtered_sets)} sets")

    # Create data files
    print(f"\nCreating data files...")
    create_data_files(filtered_sets)

    # Print statistics
    print_dataset_statistics(filtered_sets)

    # Extract themes from filtered sets
    print(f"\nExtracting themes...")
    themes, parent_themes = extract_themes(filtered_sets)
    print(f"  Found {len(themes)} unique themes")
    print(f"  Found {len(parent_themes)} unique parent themes")

    # Create theme options for multiselect
    theme_options = [{theme: theme} for theme in themes]
    parent_theme_options = [{parent_theme: parent_theme} for parent_theme in parent_themes]

    # Create the custom fields
    custom_fields = []

    # About field
    about_field = {
        'keyname': 'about',
        'name': 'About This Plugin',
        'field_type': 'author_bio',
        'description': f"Display LEGO sets on your TRMNL device with flexible filtering and display options.<br /><br />"
                       f"<strong>Dataset:</strong><br />"
                       f"● {round(len(filtered_sets), -3):,}+ curated LEGO sets from <a href='https://rebrickable.com/'>Rebrickable.com</a><br />"
                       f"● Sets are sorted by release year, then by set number<br />"
                       f"● Non-LEGO items (watches, bags, etc.), single-piece sets and sets without valid images are excluded<br /><br />"
                       f"<strong>Display Options:</strong><br />"
                       f"● <strong>Random:</strong> Show a different set each refresh<br />"
                       f"● <strong>Incremental:</strong> Progress through sets chronologically<br />"
                       f"● <strong>Reverse Incremental:</strong> Progress backwards from newest to oldest<br /><br />"
                       f"<strong>Filtering Options:</strong><br />"
                       f"● Filter by release year (min/max)<br />"
                       f"● Filter by parent theme ({len(parent_themes)} available) or specific theme ({len(themes)} available)<br />"
                       f"● Combine filters to create your perfect collection",
        'github_url': 'https://github.com/ExcuseMi/trmnl-lego-plugin'
    }
    custom_fields.append(about_field)

    # Display order field
    display_order_field = {
        'keyname': 'display_order',
        'name': 'Display Order',
        'field_type': 'select',
        'description': 'Choose the order in which sets are displayed on your device.',
        'options': [
            {'Random (shuffle each refresh)': 'random'},
            {'Chronological (oldest to newest)': 'incremental'},
            {'Reverse Chronological (newest to oldest)': 'reverse_incremental'}
        ],
        'default': 'random',
        'optional': True
    }
    custom_fields.append(display_order_field)

    # Parent themes multiselect field
    parent_themes_field = {
        'keyname': 'parent_themes',
        'field_type': 'select',
        'name': f'Filter by Parent Themes ({len(parent_themes)} available)',
        'description': 'Select one or more parent themes (broad categories like "Star Wars" or "City"). Leave empty to show all parent themes.',
        'multiple': True,
        'help_text': 'Use <kbd>⌘</kbd>+<kbd>click</kbd> (Mac) or <kbd>ctrl</kbd>+<kbd>click</kbd> (Windows) to select multiple items. Use <kbd>Shift</kbd>+<kbd>click</kbd> to select a whole range at once.',
        'options': parent_theme_options,
        'optional': True
    }
    custom_fields.append(parent_themes_field)

    # Themes multiselect field
    themes_field = {
        'keyname': 'themes',
        'field_type': 'select',
        'name': f'Filter by Specific Themes ({len(themes)} available)',
        'description': 'Select one or more specific themes (sub-categories like "The Mandalorian" or "Police"). Leave empty to show all themes.',
        'multiple': True,
        'help_text': 'Use <kbd>⌘</kbd>+<kbd>click</kbd> (Mac) or <kbd>ctrl</kbd>+<kbd>click</kbd> (Windows) to select multiple items. Use <kbd>Shift</kbd>+<kbd>click</kbd> to select a whole range at once.',
        'options': theme_options,
        'optional': True
    }
    custom_fields.append(themes_field)

    # Min year field
    min_year_field = {
        'keyname': 'min_year',
        'field_type': 'number',
        'name': 'Minimum Release Year',
        'description': 'Only show sets released in or after this year.<br /><i>Leave empty to use no restrictions.</i>',
        'min': 1900,
        'max': 2050,
        'placeholder': '1970',
        'optional': True
    }
    custom_fields.append(min_year_field)

    # Max year field
    max_year_field = {
        'keyname': 'max_year',
        'field_type': 'number',
        'name': 'Maximum Release Year',
        'description': 'Only show sets released in or before this year.<br /><i>Leave empty to use no restrictions.</i>',
        'min': 1900,
        'max': 2050,
        'placeholder': '2025',
        'optional': True
    }
    custom_fields.append(max_year_field)

    # Use custom YAML representer to format the output properly
    def represent_dict_order(dumper, data):
        return dumper.represent_mapping('tag:yaml.org,2002:map', data.items())

    yaml.add_representer(dict, represent_dict_order)

    print(f"\nWriting options to: {OUTPUT_FILE.absolute()}")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        yaml.dump(custom_fields, f, default_flow_style=False, allow_unicode=True, sort_keys=False, width=1000)

    print(f"  ✓ Successfully created {OUTPUT_FILE.name}")

    # Print summary
    print(f"\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Files created:")
    print(f"  • {REDUCED_SETS_JSON.name} - Filtered sets in JSON format")
    print(f"  • {REDUCED_SETS_TXT.name} - Filtered sets in TXT format (for plugin)")
    print(f"  • {OUTPUT_FILE.name} - Plugin options configuration")
    print(f"\nDataset summary:")
    print(f"  • Total sets in filtered dataset: {len(filtered_sets)}")
    print(f"  • Total parent themes: {len(parent_themes)}")
    print(f"  • Total specific themes: {len(themes)}")

    # Show sample of parent themes
    print(f"\nSample parent themes (first 10):")
    for i, theme in enumerate(parent_themes[:10]):
        print(f"  {i + 1}. {theme}")

    if len(parent_themes) > 10:
        print(f"  ... and {len(parent_themes) - 10} more")

    # Show sample of themes
    print(f"\nSample specific themes (first 10):")
    for i, theme in enumerate(themes[:10]):
        print(f"  {i + 1}. {theme}")

    if len(themes) > 10:
        print(f"  ... and {len(themes) - 10} more")


if __name__ == "__main__":
    create_options_yml()