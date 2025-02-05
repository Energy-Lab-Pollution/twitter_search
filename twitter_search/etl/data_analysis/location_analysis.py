"""
First location analysis try
"""

import re
from pathlib import Path

import pandas as pd


# TODO: get this constants from general utils folder

script_path = Path(__file__).resolve()
project_root = script_path.parents[2]
CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"
ANALYSIS_OUTPUT = project_root / "data" / "analysis_outputs"

ALIAS_DICT = {
    "mexico city": [
        "cdmx",
        "ciudad de mexico",
        "distrito federal",
        "mexico df",
    ],
    "bangalore": ["bengaluru"],
    "guatemala": ["ciudad de guatemala"],
}


def check_location(raw_location, target_location):
    """
    Uses regex to see if the raw location matches
    the target location
    """
    if target_location in ALIAS_DICT:
        target_locations = ALIAS_DICT[target_location]
        target_locations.append(target_location)
    else:
        target_locations = [target_location]

    if isinstance(raw_location, str):
        raw_location = raw_location.lower().strip()
        location_regex = re.findall(r"\w+", raw_location)

        if location_regex:
            for target_location in target_locations:
                if target_location in location_regex:
                    return True
                elif target_location in raw_location:
                    return True
            else:
                return False
        else:
            return False
    else:
        return False


def user_counting(users_df):
    """
    Counts the number of matched users per location
    and classification
    """

    match_group = users_df.groupby(
        by=["location_match", "search_location"]
    ).count()
    match_group.reset_index(drop=False, inplace=True)
    match_group = match_group.loc[
        :, ["user_id", "search_location", "location_match"]
    ]
    match_group.sort_values(by="search_location", inplace=True)
    match_group.rename(columns={"user_id": "user_count"}, inplace=True)

    match_pivot = match_group.pivot_table(
        index="search_location", columns="location_match", values="user_count"
    )

    return match_pivot


if __name__ == "__main__":
    default_users = pd.read_csv(
        f"{CLEAN_DATA_PATH}/all_distinct_users.csv", encoding="utf-8-sig"
    )

    default_users.loc[:, "location_match"] = default_users.apply(
        lambda x: check_location(x.location, x.search_location), axis=1
    )

    default_users = default_users.loc[
        :, ["user_id", "location", "search_location", "location_match"]
    ]

    default_users.to_csv(
        f"{ANALYSIS_OUTPUT}/location_matches.csv",
        encoding="utf-8-sig",
        index=False,
    )

    user_counts = user_counting(default_users)

    user_counts.to_csv(
        f"{ANALYSIS_OUTPUT}/location_matches_counts.csv", encoding="utf-8-sig"
    )
