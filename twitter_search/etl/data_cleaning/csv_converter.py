"""
This script converts the JSON files into CSV files for easier data manipulation.
"""

import json
import os
from pathlib import Path

# General imports
import pandas as pd
from config_utils.cities import ALIAS_DICT


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
    "search_account_type",
    "tweet_date",
]


class CSVConverter:
    FILETYPE_INDEX = 4
    ACCOUNT_TYPE_COL = "search_account_type"

    def __init__(self, location, twikit=False) -> None:
        # See which JSON files are available
        self.location = location.lower()
        self.twikit = twikit

        print("Checking if city is in secondary cities dictionary")
        if self.location in ALIAS_DICT:
            print(f"{self.location} found in alias dict")
            self.location = ALIAS_DICT[self.location]

        # JSON files parsing
        self.build_paths()
        self.get_json_files()
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

    def build_paths(self):
        """
        Builds the according paths depeding on if the
        Twikit extraction is being used or not
        """

        if self.twikit:
            self.raw_data_path = project_root / "data" / "twikit_raw_data"
            self.clean_data_path = project_root / "data" / "twikit_cleaned_data"

        else:
            self.raw_data_path = project_root / "data" / "raw_data"
            self.clean_data_path = project_root / "data" / "cleaned_data"

    def get_json_files(self):
        """
        Method that gets the json files, depending on whether
        Twikit is being used or not
        """
        if not self.twikit:
            self.json_files = os.listdir(self.raw_data_path)
        else:
            # Twikit has a subdirectory per date
            self.json_files = []
            directories = os.listdir(self.raw_data_path)
            for directory in directories:
                files = os.listdir(self.raw_data_path / directory)
                files = [f"{directory}/{file}" for file in files]
                self.json_files.extend(files)

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
            if "users" in file.lower()
            and "filtered" in file.lower()
            and "expanded" not in file.lower()
        ]

        self.list_files = [
            file for file in self.filtered_files if "list" in file.lower()
        ]

        self.expanded_user_files = [
            file
            for file in self.filtered_files
            if "expanded" in file.lower() and "filtered" in file.lower()
        ]

    @staticmethod
    def flatten_and_remove_empty(input_list):
        """
        Flatten a list of lists into a single list and remove any empty lists within it.

        Args:
            input_list (list): The list of lists to be flattened and cleaned.

        Returns:
            list: The flattened list with empty lists removed.
        """
        new_list = []
        for item in input_list:
            if isinstance(item, list):
                subitems = [subitem for subitem in item]
                new_list.extend(subitems)
            else:
                new_list.append(item)

        return new_list

    def convert_users_to_df(self, input_file):
        """
        Convert JSON files into CSV files.

        Args:
            input_file (str): The input JSON file.
            output_file (str): The output CSV file.

        Returns:
            None
        """
        # Get file type
        filename_split = str(input_file).split("_")
        filetype = filename_split[self.FILETYPE_INDEX]

        # Load the JSON file
        with open(input_file, "r") as json_file:
            data = json.load(json_file)

        # Preliminary cleaning! - if not dictionary, byeee
        data = [
            item
            for item in data
            if isinstance(item, dict) or isinstance(item, list)
        ]

        # Remove nested lists to avoid bugs
        data = self.flatten_and_remove_empty(data)

        if not data:
            return pd.DataFrame([])

        # Convert the JSON data into a DataFrame
        # If nested list
        if isinstance(data[0], list):
            df = pd.DataFrame([])
            for sub_list in data:
                if sub_list:
                    print(sub_list)
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

        df[self.ACCOUNT_TYPE_COL] = filetype
        return df

    def concat_user_dataframes(self, files, file_type):
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
            input_file = self.raw_data_path / file
            input_df = self.convert_users_to_df(input_file)

            if self.file_type_column[file_type] in input_df.columns:
                # Add date column if not available
                if "tweet_date" not in input_df.columns:
                    input_df.loc[:, "tweet_date"] = None
                if "tweet_count" not in input_df.columns:
                    input_df.loc[:, "tweet_count"] = None

                input_df = input_df.loc[:, self.user_columns]
                df = pd.concat([df, input_df], ignore_index=True)

                print(f"Data loaded successfully from {file}")

        df.loc[:, "search_location"] = self.location

        return df

    def convert_lists_to_df(self, input_file):
        """
        Converts the lists data into a dataframe
        """
        # Load the JSON file
        with open(input_file, "r") as json_file:
            data = json.load(json_file)

        df = pd.DataFrame.from_records(data)

        return df

    def concat_list_dataframes(self, files, file_type):
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
            input_file = self.raw_data_path / file
            input_df = self.convert_lists_to_df(input_file)

            if self.location == "manually_added":
                df = pd.concat([df, input_df], ignore_index=True)

            elif self.file_type_column[file_type] in input_df.columns:
                df = pd.concat([df, input_df], ignore_index=True)

                print(f"Data loaded successfully from {file}")

        df.loc[:, "search_location"] = self.location

        return df

    def parse_user_df(self, user_type):
        """
        Pre-processes, parses and saves the users dataframe

        Args:
            - user_type: can either be "normal" or "expanded"

        Note that the expanded users were obtained via list expansion
        """

        if user_type == "normal":
            user_df = self.concat_user_dataframes(
                self.user_files, file_type="user"
            )
            expanded = False
            filename = f"{self.location}_user_data.csv"
            unique_filename = f"{self.location}_unique_users.csv"
        else:
            user_df = self.concat_user_dataframes(
                self.expanded_user_files, file_type="user"
            )
            expanded = True
            filename = f"{self.location}_expanded_user_data.csv"
            unique_filename = f"{self.location}_expanded_unique_user.csv"

        # Drop columns that are not needed
        # Get the user URL
        user_df.dropna(subset=["user_id"], inplace=True)
        user_df.dropna(subset=["content_is_relevant"], inplace=True)
        user_df.loc[:, "user_url"] = user_df["username"].apply(
            lambda x: self.create_user_url(x)
        )
        user_df.loc[:, "list_expansion"] = expanded

        unique_users = user_df.drop_duplicates(subset=["user_id"])
        unique_users.to_csv(
            self.clean_data_path / unique_filename,
            index=False,
            encoding="utf-8-sig",
        )

        user_df.to_csv(
            self.clean_data_path / filename,
            index=False,
            encoding="utf-8-sig",
        )

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
            self.parse_user_df(user_type="normal")
            print(f"Normal users found for {self.location}")

        if self.expanded_user_files:
            self.parse_user_df(user_type="expanded")
            print(f"Expanded users found for {self.location}")

        if self.list_files:
            list_df = self.concat_list_dataframes(
                self.list_files, file_type="list"
            )
            list_df.dropna(subset=["relevant"], inplace=True)
            list_df.to_csv(
                self.clean_data_path / f"{self.location}_list_data.csv",
                index=False,
                encoding="utf-8-sig",
            )
            print(f"List data saved successfully for {self.location}")

        else:
            print(f"No list data found for {self.location}")
