"""
This script converts the JSON files into CSV files for easier data manipulation.
"""

import json
import os
from pathlib import Path

# General imports
import pandas as pd


script_path = Path(__file__).resolve()
project_root = script_path.parents[2]

USER_COLUMNS = [
    "user_id",
    "username",
    "name",
    "description",
    "location",
    "tweet_count",
    "followers_count",
    "following_count",
    "listed_count",
    "tweets",
    "content_is_relevant",
    "content_labels",
    "tweet_date",
]


class CSVConverter:
    # Construct the path to the cleaned_data directory
    RAW_DATA_PATH = project_root / "data" / "raw_data"
    CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"

    def __init__(self, location) -> None:
        # See which JSON files are available
        self.json_files = os.listdir(self.RAW_DATA_PATH)
        self.location = location
        self.filter_json_files()

        self.file_type_column = {
            "user": "content_is_relevant",
            "list": "relevant",
        }

        self.user_columns = USER_COLUMNS

    @staticmethod
    def create_user_url(username):
        """
        Create the URL for a user based on their username.

        Args:
            username (str): The username of the user.

        Returns:
            str: The URL of the user.
        """
        return f"https://twitter.com/{username}"

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
            for file in self.filtered_files
            if "users" in file.lower() and "filtered" in file.lower()
        ]

        self.list_files = [
            file for file in self.filtered_files if "list" in file.lower()
        ]

    @staticmethod
    def convert_to_df(input_file):
        """
        Convert JSON files into CSV files.

        Args:
            input_file (str): The input JSON file.
            output_file (str): The output CSV file.

        Returns:
            None
        """
        # Load the JSON file
        with open(input_file, "r") as json_file:
            data = json.load(json_file)

        # Preliminary cleaning! - if not dictionary, byeee
        data = [
            item
            for item in data
            if isinstance(item, dict) or isinstance(item, list)
        ]
        if not data:
            return pd.DataFrame([])

        # Convert the JSON data into a DataFrame
        # If nested list
        if isinstance(data[0], list):
            df = pd.DataFrame([])
            for sub_list in data:
                if sub_list:
                    if isinstance(sub_list, str):
                        continue
                    try:
                        sub_df = pd.DataFrame.from_records(sub_list)
                        df = pd.concat([df, sub_df], ignore_index=True)
                    except Exception as error:
                        print(
                            f"Error parsing dataframe from records, trying as from_dict: {error}"
                        )

                        try:
                            sub_df = pd.DataFrame.from_dict(
                                sub_list, orient="index"
                            )
                            df = pd.concat([sub_df, df], ignore_index=True)
                        except Exception as error:
                            print(f"Error parsing as df with dict: {error}")
                        continue
                else:
                    continue

        else:
            try:
                df = pd.DataFrame.from_records(data)
            except Exception as error:
                print(error)
                df = pd.DataFrame(data)

        return df

    def concat_dataframes(self, files, file_type):
        """
        Reads the JSON files, creates a dataframe
        for each file and concatenates all the dataframes.

        Args:
            files_list (list): List of JSON files.

        Returns:
            DataFrame: The concatenated DataFrame.
        """

        if file_type not in self.file_type_column:
            raise ValueError(
                f"""File type {file_type} not recognized,must be
                              one from {self.file_type_column.keys()}"""
            )

        df = pd.DataFrame()

        for file in files:
            input_file = self.RAW_DATA_PATH / file
            input_df = self.convert_to_df(input_file)

            if self.file_type_column[file_type] in input_df.columns:
                # Add date column if not available
                if "tweet_date" not in input_df.columns:
                    input_df.loc[:, "tweet_date"] = None
                input_df = input_df.loc[:, self.user_columns]
                df = pd.concat([df, input_df], ignore_index=True)

                print(f"Data loaded successfully from {file}")

        df.loc[:, "search_location"] = self.location

        return df

    def run(self):
        """
        Runs the entire process for converting JSON files
        for a certain location, into DataFrames and then
        concatenating them. This process runs for both
        user and list data.

        Args:
            location (str): The location to filter on.

        Returns:
            None
        """
        if self.user_files:
            user_df = self.concat_dataframes(self.user_files, file_type="user")
            # Drop columns that are not needed
            # Get the user URL
            user_df.loc[:, "user_url"] = user_df["username"].apply(
                lambda x: self.create_user_url(x)
            )

            user_df.to_csv(
                self.CLEAN_DATA_PATH / f"{self.location}_user_data.csv",
                index=False,
                encoding="utf-8-sig",
            )
            print(f"User data saved successfully for {self.location}")

        else:
            print(f"No user data found for {self.location}")

        if self.list_files:
            list_df = self.concat_dataframes(self.list_files, file_type="list")
            list_df.dropna(subset=["relevant"], inplace=True)
            list_df.to_csv(
                self.CLEAN_DATA_PATH / f"{self.location}_list_data.csv",
                index=False,
                encoding="utf-8-sig",
            )
            print(f"List data saved successfully for {self.location}")

        else:
            print(f"No list data found for {self.location}")
