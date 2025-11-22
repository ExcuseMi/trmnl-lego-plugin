import json
from pathlib import Path
import yaml

# Get the script's directory and navigate to project root
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
SETS_FILE = DATA_DIR / "sets.json"
OUTPUT_FILE = DATA_DIR / "options.yml"


def load_sets():
    """Load sets data from JSON file"""
    if not SETS_FILE.exists():
        print(f"Error: {SETS_FILE} not found")
        return None

    with open(SETS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


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


def create_options_yml():
    print("=" * 60)
    print("LEGO Plugin Options Generator")
    print("=" * 60)

    # Load data
    sets = load_sets()

    if not sets:
        print("\nFailed to load required data. Exiting.")
        return

    print(f"\nLoaded {len(sets)} LEGO sets")

    # Extract themes
    themes, parent_themes = extract_themes(sets)
    print(f"Found {len(themes)} unique themes")
    print(f"Found {len(parent_themes)} unique parent themes")

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
                       f"● {round(len(sets), -3):,}+ curated LEGO sets from <a href='https://rebrickable.com/'>Rebrickable.com</a><br />"
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

    # Selection mode field
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

    print(f"\nWriting to: {OUTPUT_FILE.absolute()}")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        yaml.dump(custom_fields, f, default_flow_style=False, allow_unicode=True, sort_keys=False, width=1000)

    print(f"✓ Successfully created {OUTPUT_FILE}")

    # Print summary
    print(f"\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total LEGO sets: {len(sets)}")
    print(f"Total parent themes: {len(parent_themes)}")
    print(f"Total specific themes: {len(themes)}")

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