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
        self.CLEAN_DATA_PATH = self.project_root / "data" / "cleaned_data"

        self.csv_files = os.listdir(self.CLEAN_DATA_PATH)

        # The dict contains a string to look for in the csv files
        # and the final name of the concatenated csv file
        self.file_dict = {
            "user_data": "all_users",
            "unique_users": "all_distinct_users",
        }

    def concat_files(self, str_to_look, final_file):
        """
        Concatenates all of the csv files

        Reads all of the available files and concatenates them
        """

        user_files = [file for file in self.csv_files if str_to_look in file]

        all_users = pd.DataFrame()

        for csv_file in user_files:
            df = pd.read_csv(f"{self.CLEAN_DATA_PATH}/{csv_file}")
            all_users = pd.concat([all_users, df], ignore_index=True)

        all_users.to_csv(
            f"{self.CLEAN_DATA_PATH}/{final_file}.csv",
            index=False,
            encoding="utf-8-sig",
        )

        print("Successfully concatenated all csv files")

    def run(self):
        """
        Concats all of the csv files
        """

        for str_to_look in self.file_dict.keys():
            self.concat_files(str_to_look, self.file_dict[str_to_look])


if __name__ == "__main__":
    csv_concat = CSVConcat()
    csv_concat.run()
