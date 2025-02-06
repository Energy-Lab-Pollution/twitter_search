"""
First location analysis try
"""

import re

import pandas as pd
from config_utils.cities import ALIAS_DICT
from config_utils.constants import analysis_project_root


class LocationAnalyzer:
    def __init__(self):
        self.CLEAN_DATA_PATH = analysis_project_root / "data" / "cleaned_data"
        self.ANALYSIS_OUTPUT = analysis_project_root / "data" / "analysis_outputs"

        self.users = pd.read_csv(
            f"{self.CLEAN_DATA_PATH}/all_distinct_users.csv",
            encoding="utf-8-sig",
        )

    @staticmethod
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

    def user_counting(self):
        """
        Counts the number of matched users per location
        and classification
        """

        match_group = self.users.groupby(
            by=["location_match", "search_location"]
        ).count()
        match_group.reset_index(drop=False, inplace=True)
        match_group = match_group.loc[
            :, ["user_id", "search_location", "location_match"]
        ]
        match_group.sort_values(by="search_location", inplace=True)
        match_group.rename(columns={"user_id": "user_count"}, inplace=True)

        match_pivot = match_group.pivot_table(
            index="search_location",
            columns="location_match",
            values="user_count",
        )

        return match_pivot

    def run(self):
        """
        Runs the entire location analysis pileine
        """
        self.users.loc[:, "location_match"] = self.users.apply(
            lambda x: self.check_location(x.location, x.search_location), axis=1
        )
        self.users = self.users.loc[
            :, ["user_id", "location", "search_location", "location_match"]
        ]

        self.users.to_csv(
            f"{self.ANALYSIS_OUTPUT}/location_matches.csv",
            encoding="utf-8-sig",
            index=False,
        )

        user_counts = self.user_counting()

        user_counts.to_csv(
            f"{self.ANALYSIS_OUTPUT}/location_matches_counts.csv",
            encoding="utf-8-sig",
        )
