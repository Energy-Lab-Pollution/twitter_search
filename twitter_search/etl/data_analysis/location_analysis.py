"""
First location analysis try
"""

from pathlib import Path

import pandas as pd

script_path = Path(__file__).resolve()
project_root = script_path.parents[2]
CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"

# Read both files

default_users = pd.read_csv(f"{CLEAN_DATA_PATH}/all_distinct_users.csv",
                            encoding='utf-8-sig')

expanded_users = pd.read_csv(f"{CLEAN_DATA_PATH}/expanded_distinct_users.csv",
                             encoding='utf-8-sig')


all_users = pd.concat([default_users, expanded_users],
                      ignore_index=True)

# print(all_users.info())

print(all_users.head())
