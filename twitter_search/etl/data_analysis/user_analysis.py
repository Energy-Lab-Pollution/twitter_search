"""
Does an analysis of the all the collected users
"""

import pandas as pd
from pathlib import Path

script_path = Path(__file__).resolve()
project_root = script_path.parents[2]
CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"


all_users_df = pd.read_csv(
    f"{CLEAN_DATA_PATH}/all_users.csv", encoding="utf-8-sig"
)

# Analyze user types

user_types = all_users_df.groupby(
    by=["search_location", "search_account_type"]
).count()

print(user_types)
