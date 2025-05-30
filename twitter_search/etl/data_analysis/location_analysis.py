"""
First location analysis try
"""

import re

import pandas as pd
from config_utils.cities import ALIAS_DICT
from config_utils.constants import analysis_project_root


class LocationAnalyzer:
    EDGE_CASE = "guatemala"

    def __init__(self):
        self.ANALYSIS_OUTPUT = (
            analysis_project_root / "data" / "analysis_outputs"
        )
        self.MASTER_DATASET_PATH = (
            analysis_project_root / "data" / "master_dataset"
        )
        self.users = pd.read_csv(
            f"{self.MASTER_DATASET_PATH}/all_distinct_users.csv",
            encoding="utf-8-sig",
        )

    def check_location(self, raw_location, target_location):
        """
        Uses regex to see if the raw location matches
        the target location
        """

        # We dont want to consider 'guatemala' in this case
        if target_location == self.EDGE_CASE:
            target_locations = []
        else:
            target_locations = [target_location]
        # alias is the key, target loc is the value
        for alias, value in ALIAS_DICT.items():
            if value == target_location:
                target_locations.append(alias)

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

        # Reset index and get additional calculations
        match_pivot.reset_index(drop=False, inplace=True)
        match_pivot.columns = ["Location", "False", "True"]
        match_pivot.loc[:, "Total"] = (
            match_pivot.loc[:, "True"] + match_pivot.loc[:, "False"]
        )
        match_pivot.loc[:, "Match_Percentage"] = (
            match_pivot.loc[:, "True"] / match_pivot.loc[:, "Total"]
        )
        match_pivot.loc[:, "Match_Percentage"] = match_pivot.loc[
            :, "Match_Percentage"
        ].apply(lambda x: round(x, 2))

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
            index=False,
        )
