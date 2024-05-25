"""
Script with constants for the list filtering process
"""

# Global imports

from pathlib import Path

# Lists_filtering constants

script_path = Path(__file__).resolve()
project_root = script_path.parents[2]

# Construct the path to the cleaned_data directory
RAW_DATA_PATH = project_root / "data" / "raw_data"
CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"

COLS_TO_KEEP = ["user_id", "list_id", "name", "description"]
LISTS_KEYWORDS = [
    "air",
    "pollution",
    "earth",
    "climate",
    "smog",
    "science",
    "research",
    "researchers",
    "politics",
    "politicians",
    "media",
    "journalists",
]
