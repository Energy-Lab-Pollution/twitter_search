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

    def concat_files(self):
        """
        Concatenates all of the csv files

        Reads all of the available files and concatenates them
        """

        csv_files = os.listdir(self.CLEAN_DATA_PATH)
        csv_files = [file for file in csv_files if "user_data" in file]

        all_users = pd.DataFrame()

        for csv_file in csv_files:
            df = pd.read_csv(f"{self.CLEAN_DATA_PATH}/{csv_file}")
            all_users = pd.concat([all_users, df], ignore_index=True)

        all_users.to_csv(
            f"{self.CLEAN_DATA_PATH}/all_users.csv",
            index=False,
            encoding="utf-8-sig",
        )

        print("Successfully concatenated all csv files")


if __name__ == "__main__":
    csv_concat = CSVConcat()
    # concat_files()
