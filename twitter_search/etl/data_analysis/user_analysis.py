"""
Does an analysis of the all the collected users
"""

from pathlib import Path

import pandas as pd


script_path = Path(__file__).resolve()
project_root = script_path.parents[2]
CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"
ANALYSIS_OUTPUT = project_root / "data" / "analysis_outputs"
MASTER_DATASET_PATH = project_root / "data" / "master_dataset"
NUM_SAMPLE = 1200
RANDOM_STATE = 1236

all_users_df = pd.read_csv(
    f"{CLEAN_DATA_PATH}/all_distinct_users.csv", encoding="utf-8-sig"
)

master_df = pd.read_csv(
    f"{MASTER_DATASET_PATH}/master_dataset.csv", encoding="utf-8-sig"
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


def get_user_classifications_by_city(all_users_df):
    """
    Gets total number of users, distinguished by type and city
    """
    user_types = all_users_df.groupby(
        by=["search_location", "content_labels"]
    ).count()
    user_types.reset_index(drop=False, inplace=True)
    user_types.rename(columns={"user_id": "count"}, inplace=True)
    user_types = user_types.loc[
        :, ["search_location", "content_labels", "count"]
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


def get_percentages(user_types, user_cities, filename):
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
    final_df.to_csv(f"{ANALYSIS_OUTPUT}/{filename}", index=True)

    return final_df


def generate_random_sample(all_users_df):
    """
    Generates a random sample of the users dataframe
    """

    random_sample = all_users_df.sample(n=NUM_SAMPLE, random_state=RANDOM_STATE)
    random_sample.to_csv(
        f"{ANALYSIS_OUTPUT}/random_sample.csv",
        index=False,
        encoding="utf-8-sig",
    )


if __name__ == "__main__":
    datasets = [all_users_df, master_df]
    filenames = ["user_analysis.csv", "user_analysis_with_expansionss.csv"]

    for dataset, filename in zip(datasets, filenames):
        user_classifications = get_user_classifications_by_city(dataset)
        user_cities = get_users_per_city(dataset)

        final_classification_df = get_percentages(
            user_classifications, user_cities, filename
        )
