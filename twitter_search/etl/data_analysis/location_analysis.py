"""
First location analysis try
"""

import re
from pathlib import Path

import pandas as pd

script_path = Path(__file__).resolve()
project_root = script_path.parents[2]
CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"


def check_location(raw_location, target_location):
    """
    Uses regex to see if the raw location matches
    the target location
    """
    raw_location = target_location.strip()
    target_location = target_location.lower().strip()

    if target_location in raw_location:
        return True
    else:
        return False


default_users = pd.read_csv(
    f"{CLEAN_DATA_PATH}/all_distinct_users.csv", encoding="utf-8-sig"
)


target_locations = list(default_users.loc[:, "search_location"].unique())
raw_locations = list(default_users.loc[:, "location"].unique())

print(raw_locations)