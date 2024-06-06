"""
Detects all generated .csv files and concatenates them into a single file
"""

import os
import pandas as pd
from pathlib import Path


script_path = Path(__file__).resolve()
project_root = script_path.parents[1]

print(project_root)

CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"


def concat_files():
    """
    Concatenates all of the csv files

    Reads all of the available files and concatenates them
    """

    csv_files = os.listdir(CLEAN_DATA_PATH)
    csv_files = [file for file in csv_files if ".csv" in file]
    csv_files = [
        file for file in csv_files if "user" in file and "all_users" not in file
    ]

    all_users = pd.DataFrame()

    for csv_file in csv_files:
        df = pd.read_csv(f"{CLEAN_DATA_PATH}/{csv_file}")
        all_users = pd.concat([all_users, df], ignore_index=True)

    all_users.to_csv(
        f"{CLEAN_DATA_PATH}/all_users.csv", index=False, encoding="utf-8-sig"
    )

    print("Successfully concatenated all csv files")


if __name__ == "__main__":
    concat_files()
