"""
Detects all generated .csv files and concatenates them into a single file
"""

import os
from pathlib import Path

import pandas as pd


class CSVConcat:
    def __init__(self):
        self.script_path = Path(__file__).resolve()
        self.project_root = self.script_path.parents[1]

        # Configure paths
        self.CLEAN_DATA_PATH = self.project_root / "data" / "cleaned_data"
        self.TWIKIT_CLEAN_DATA_PATH = (
            self.project_root / "data" / "twikit_cleaned_data"
        )
        self.MASTER_DATA_PATH = self.project_root / "data" / "master_dataset"

        # Gather csv files
        csv_files = os.listdir(self.CLEAN_DATA_PATH)
        twikit_csv_files = os.listdir(self.TWIKIT_CLEAN_DATA_PATH)

        self.csv_files = [self.CLEAN_DATA_PATH / file for file in csv_files]
        twikit_csv_files = [
            self.TWIKIT_CLEAN_DATA_PATH / file for file in twikit_csv_files
        ]

        # All files are now in a single list
        self.csv_files.extend(twikit_csv_files)

        # The dict contains a string to look for in the csv files
        # and the final name of the concatenated csv file
        self.file_dict = {
            "user_data": "all_users",
            "unique_users": "all_distinct_users",
            "expanded_user_data": "expanded_all_users",
            "expanded_unique_user": "expanded_distinct_users",
        }

        # Dictionary with strings to avoid when looking for
        # the corresponding files
        self.str_to_avoid_dict = {
            "user_data": "expanded",
            "unique_users": "expanded",
            "expanded_user_data": None,
            "expanded_unique_user": None,
        }

    def concat_files(self, str_to_look, str_to_avoid, final_file):
        """
        Concatenates all of the csv files

        Reads all of the available files and concatenates them
        """

        if str_to_avoid:
            user_files = [
                file
                for file in self.csv_files
                if str_to_look in str(file) and str_to_avoid not in str(file)
            ]

        else:
            user_files = [
                file for file in self.csv_files if str_to_look in str(file)
            ]

        all_users = pd.DataFrame()

        for csv_file in user_files:
            df = pd.read_csv(csv_file)
            all_users = pd.concat([all_users, df], ignore_index=True)

        # For now, saving the csv files in the 'CLEAN_DATA_PATH' folder
        all_users.to_csv(
            f"{self.MASTER_DATA_PATH}/{final_file}.csv",
            index=False,
            encoding="utf-8-sig",
        )

        print("Successfully concatenated all csv files")

    def generate_master_dataset(self):
        """
        Concatenates distinct users
        """
        normal_distinct_users = pd.read_csv(
            f"{self.MASTER_DATA_PATH}/all_distinct_users.csv",
            encoding="utf-8-sig",
        )
        expanded_distinct_users = pd.read_csv(
            f"{self.MASTER_DATA_PATH}/expanded_distinct_users.csv",
            encoding="utf-8-sig",
        )

        master_dataset = pd.concat(
            [normal_distinct_users, expanded_distinct_users], ignore_index=True
        )

        if not os.path.exists(self.MASTER_DATA_PATH):
            print("Creating path for master dataset...")
            os.makedirs(self.MASTER_DATA_PATH)

        master_dataset.to_csv(
            f"{self.MASTER_DATA_PATH}/master_dataset.csv",
            encoding="utf-8-sig",
            index=False,
        )
        print("Saved master dataset")

    def run(self):
        """
        Concats all of the csv files
        """

        for str_to_look in self.file_dict.keys():
            str_to_avoid = self.str_to_avoid_dict[str_to_look]
            self.concat_files(
                str_to_look, str_to_avoid, self.file_dict[str_to_look]
            )

        self.generate_master_dataset()

