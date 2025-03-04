"""
Does an analysis of the all the collected users
"""

import numpy as np
import pandas as pd
from config_utils.constants import analysis_project_root


class UserAnalyzer:
    NUM_SAMPLE = 1200
    RANDOM_STATE = 1236

    def __init__(self):
        self.CLEAN_DATA_PATH = analysis_project_root / "data" / "cleaned_data"
        self.ANALYSIS_OUTPUT = (
            analysis_project_root / "data" / "analysis_outputs"
        )
        self.MASTER_DATASET_PATH = (
            analysis_project_root / "data" / "master_dataset"
        )

        self.all_users_df = pd.read_csv(
            f"{self.CLEAN_DATA_PATH}/all_distinct_users.csv",
            encoding="utf-8-sig",
        )

        self.master_df = pd.read_csv(
            f"{self.MASTER_DATASET_PATH}/master_dataset.csv",
            encoding="utf-8-sig",
        )

    @staticmethod
    # Analyze user types
    def get_user_types_by_city(users_df):
        """
        Gets total number of users, distinguished by type and city
        """
        user_types = users_df.groupby(
            by=["search_location", "search_account_type"]
        ).count()
        user_types.reset_index(drop=False, inplace=True)
        user_types.rename(columns={"user_id": "count"}, inplace=True)
        user_types = user_types.loc[
            :, ["search_location", "search_account_type", "count"]
        ]

        return user_types

    @staticmethod
    def get_user_classifications_by_city(users_df):
        """
        Gets total number of users, distinguished by location
        and tbe classifications done by the model.

        Args:
            - users_df(pd.DataFrame): Disaggregated dataframe with a row
            per classificated user

        Returns:
            - user_types: pd.DataFrame with three columns: city,
            content_labels and count.
        """
        user_types = users_df.groupby(
            by=["search_location", "content_labels"]
        ).count()
        user_types.reset_index(drop=False, inplace=True)
        user_types.rename(columns={"user_id": "count"}, inplace=True)
        user_types = user_types.loc[
            :, ["search_location", "content_labels", "count"]
        ]

        return user_types

    @staticmethod
    # Get totals by city
    def get_users_per_city(users_df):
        """
        Gets total number of users per city.
        Args:
            - users_df(pd.DataFrame): Disaggregated dataframe with a row
            per classificated user

        Returns:
            - user_cities: pd.DataFrame with two columns: city and
            total count
        """
        user_cities = users_df.groupby(by=["search_location"]).count()
        user_cities.reset_index(drop=False, inplace=True)
        user_cities.rename(columns={"user_id": "total_count"}, inplace=True)
        user_cities = user_cities.loc[:, ["search_location", "total_count"]]

        return user_cities

    def get_users_count(self, user_types, filename):
        """
        Gets the total number of users per city and category. For example,
        we will get how many users belong in Chicago, and how many of those
        users were classified in each category (researchers, etc.)

        Args:
            - user_types: pd.DataFrame with three columns: city,
            content_labels and count.
        """
        # Have columns be the classifications and cities the rows
        final_df = user_types.pivot_table(
            index="search_location", values="count", columns="content_labels"
        )
        final_df.loc[:, "total_per_city"] = final_df.apply(np.sum, axis=1)
        # Note that the indices are the cities
        final_df.to_csv(f"{self.ANALYSIS_OUTPUT}/{filename}", index=True)

        return final_df

    def generate_random_sample(self, all_users_df):
        """
        Generates a random sample of the users dataframe
        """

        random_sample = all_users_df.sample(
            n=self.NUM_SAMPLE, random_state=self.RANDOM_STATE
        )
        random_sample.to_csv(
            f"{self.ANALYSIS_OUTPUT}/random_sample.csv",
            index=False,
            encoding="utf-8-sig",
        )

    def run(self):
        """
        Performs the different groupbys for the user datasets
        """

        datasets = [self.all_users_df, self.master_df]
        filenames = ["user_analysis.csv", "user_analysis_with_expansionss.csv"]

        for dataset, filename in zip(datasets, filenames):
            user_classifications = self.get_user_classifications_by_city(
                dataset
            )
            user_cities = self.get_users_per_city(dataset)
            self.get_users_count(user_classifications, user_cities, filename)
