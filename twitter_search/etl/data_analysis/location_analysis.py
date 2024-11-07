"""
First location analysis try
"""

import re
from pathlib import Path

import pandas as pd

script_path = Path(__file__).resolve()
project_root = script_path.parents[2]
CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"
ANALYSIS_OUTPUT = project_root / "data" / "analysis_outputs"


def check_location(raw_location, target_location):
    """
    Uses regex to see if the raw location matches
    the target location
    """
    if isinstance(raw_location, str):

        raw_location = raw_location.lower().strip()
        target_location = target_location.lower().strip()

        location_regex = re.findall(r"\w+", raw_location)

        if location_regex:
            if target_location in location_regex:
                return True
            else:
                return False
        else:
            return False
    else:
        return False


if __name__ == "__main__":

    default_users = pd.read_csv(
        f"{CLEAN_DATA_PATH}/all_distinct_users.csv", encoding="utf-8-sig"
    )

    default_users.loc[:, "location_match"] = default_users.apply(
        lambda x: check_location(x.location, x.search_location), axis=1
    )

    print(default_users.loc[:, ["location", "search_location", "location_match"]])

    default_users.to_csv()
