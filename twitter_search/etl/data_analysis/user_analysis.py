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
        Gets total number of users, distinguished by type and city
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
        Get total number of users per city
        """
        user_cities = users_df.groupby(by=["search_location"]).count()
        user_cities.reset_index(drop=False, inplace=True)
        user_cities.rename(columns={"user_id": "total_count"}, inplace=True)
        user_cities = user_cities.loc[:, ["search_location", "total_count"]]

        return user_cities

    def get_percentages(self, user_types, user_cities, filename):
        """
        Gets percentages of user type per city
        """
        final_df = pd.merge(
            user_types, user_cities, how="left", on="search_location"
        )

        final_df.loc[:, "percentage"] = (
            final_df.loc[:, "count"] / final_df.loc[:, "total_count"]
        )

        final_df = final_df.pivot_table(
            index="content_labels", values="count", columns="search_location"
        )
        # final_df.reset_index(drop=False, inplace=True)
        final_df = final_df.transpose()
        final_df.loc[:, "total_per_city"] = final_df.apply(np.sum, axis=1)
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

            self.get_percentages(user_classifications, user_cities, filename)
