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
def get_user_types_by_city(all_users_df):
    """
    Gets total number of users, distinguished by type and city
    """
    user_types = all_users_df.groupby(
        by=["search_location", "search_account_type"]
    ).count()
    user_types.reset_index(drop=False, inplace=True)
    user_types.rename(columns={"user_id": "count"}, inplace=True)
    user_types = user_types.loc[
        :, ["search_location", "search_account_type", "count"]
    ]

    return user_types


# Get totals by city
def get_users_per_city(all_users_df):
    """
    Get total number of users per city
    """
    user_cities = all_users_df.groupby(by=["search_location"]).count()
    user_cities.reset_index(drop=False, inplace=True)
    user_cities.rename(columns={"user_id": "total_count"}, inplace=True)
    user_cities = user_cities.loc[:, ["search_location", "total_count"]]

    return user_cities


def get_percentages(user_types, user_cities):
    """
    Gets percentages of user type per city
    """
    final_df = pd.merge(
        user_types, user_cities, how="left", on="search_location"
    )

    final_df.loc[:, "percentage"] = (
        final_df.loc[:, "count"] / final_df.loc[:, "total_count"]
    )

    final_df.to_csv(f"{CLEAN_DATA_PATH}/analysis.csv", index=False)

    return final_df


if __name__ == "__main__":

    user_types = get_user_types_by_city(all_users_df)
    user_cities = get_users_per_city(all_users_df)
    final_df = get_percentages(user_types, user_cities)

    print(final_df)
