"""
Reads the old JSONs and adds the corresponding dates from each tweet to 

"""

import os
import json
import pandas as pd

from pathlib import Path

# df = pd.read_csv(
#     "twitter_search/data/cleaned_data/all_users.csv", encoding="utf-8-sig"
# )


# df_unique = df.drop_duplicates(subset=["user_id"])

# print(df_unique)


script_path = Path(__file__).resolve()
project_root = script_path.parents[2]


class DateAdder:
    # Construct the path to the cleaned_data directory
    RAW_DATA_PATH = project_root / "data" / "raw_data"
    CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"

    def __init__(self, location):

        self.json_files = os.listdir(self.RAW_DATA_PATH)
        self.location = location
        self.filter_json_files()

        self.file_type_column = {
            "user": "content_is_relevant",
            "list": "relevant",
        }

    def filter_json_files(self):
        """
        Filter the JSON files based on the location.

        Args:
            json_files (list): The list of JSON files.
            location (str): The location to filter on.

        Returns:
            list: The filtered JSON files.
        """
        # Filter the JSON files based on the location
        self.filtered_files = [
            file
            for file in self.json_files
            if self.location.lower() in file.lower()
        ]

        self.user_files = [
            file
            for file in self.json_files
            if "users" in file.lower() in file.lower()
            and self.location.lower() in file.lower()
        ]

    def add_date_to_user(self):
        """
        If there is a date associated to any tweet, add it to the user

        If the user does not have any tweet associated to her, the
        function adds a default datetime string
        """
        # Get authors and dates from the available tweets
        tweet_authors = [tweet["author_id"] for tweet in self.total_tweets_dict]
        tweet_dates = [tweet["created_at"] for tweet in self.total_tweets_dict]

        # Dictionary of dates and authors
        authors_dates_dict = dict(zip(tweet_authors, tweet_dates))

        # Add such date to the collected users
        for user_dict in self.total_users_dict:
            user_id = user_dict["user_id"]
            author_date = authors_dates_dict.get(user_id)

            if author_date:
                # Just get 10 digits for year, month and day
                user_dict["tweet_date"] = author_date[: self.date_digits]

            else:
                user_dict["tweet_date"] = self.todays_date_str

            user_dict["user_date_id"] = f"{user_id}-{user_dict['tweet_date']}"