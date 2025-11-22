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
    """Extract unique themes from sets data and return sorted list"""
    themes = set()
    parent_themes = set()

    for set_data in sets:
        theme = set_data.get("theme")
        parent_theme = set_data.get("parent_theme")

        if theme:
            themes.add(theme)
        if parent_theme:
            parent_themes.add(parent_theme)

    # Combine and sort all unique themes
    all_themes = sorted(themes | parent_themes)

    return all_themes, len(themes), len(parent_themes)


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
    all_themes, theme_count, parent_theme_count = extract_themes(sets)
    print(f"Found {len(all_themes)} unique themes ({theme_count} themes, {parent_theme_count} parent themes)")

    # Create theme options for multiselect
    theme_options = [{theme: theme} for theme in all_themes]

    # Create the custom fields
    custom_fields = []

    # About field
    about_field = {
        'keyname': 'about',
        'name': 'About This Plugin',
        'field_type': 'author_bio',
        'description': f"Display LEGO sets on your TRMNL device with flexible filtering and display options.<br /><br />"
                       f"<strong>Dataset:</strong><br />"
                       f"● {len(sets)} curated LEGO sets from <a href='https://rebrickable.com/'>Rebrickable.com</a><br />"
                       f"● Sets are sorted by release year, then by set number<br />"
                       f"● Non-LEGO items (watches, bags, etc.) and single-piece sets are excluded<br /><br />"
                       f"<strong>Display Options:</strong><br />"
                       f"● <strong>Random:</strong> Show a different set each refresh<br />"
                       f"● <strong>Incremental:</strong> Progress through sets chronologically<br />"
                       f"● <strong>Reverse Incremental:</strong> Progress backwards from newest to oldest<br /><br />"
                       f"<strong>Filtering Options:</strong><br />"
                       f"● Filter by release year (min/max)<br />"
                       f"● Filter by theme ({len(all_themes)} themes available)<br />"
                       f"● Combine filters to create your perfect collection",
        'github_url': 'https://github.com/ExcuseMi/trmnl-lego-plugin'
    }
    custom_fields.append(about_field)

    # Selection mode field
    selection_mode_field = {
        'keyname': 's   election_mode',
        'name': 'Set Selection Mode',
        'field_type': 'select',
        'description': 'Choose how sets should be selected each time the plugin runs.',
        'options': [
            {'Random': 'random'},
            {'Incremental': 'incremental'},
            {'Reverse Incremental': 'reverse_incremental'}
        ],
        'default': 'random',
        'optional': True
    }
    custom_fields.append(selection_mode_field)

    # Themes multiselect field
    themes_field = {
        'keyname': 'themes',
        'field_type': 'select',
        'name': f'Filter by Themes ({len(all_themes)} available)',
        'description': 'Select one or more themes to filter which sets are displayed. Leave empty to show all themes.',
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
    print(f"Total unique themes: {len(all_themes)}")
    print(f"  - Themes: {theme_count}")
    print(f"  - Parent themes: {parent_theme_count}")

    # Show sample of themes
    print(f"\nSample themes (first 10):")
    for i, theme in enumerate(all_themes[:10]):
        print(f"  {i + 1}. {theme}")

    if len(all_themes) > 10:
        print(f"  ... and {len(all_themes) - 10} more")


if __name__ == "__main__":
    create_options_yml()