"""
This script converts the JSON files into CSV files for easier data manipulation.
"""

import json
from pathlib import Path
import os

# General imports
import pandas as pd


script_path = Path(__file__).resolve()
project_root = script_path.parents[2]


class CSVConverter:

    # Construct the path to the cleaned_data directory
    RAW_DATA_PATH = project_root / "data" / "raw_data"
    CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"

    def __init__(self, location) -> None:
        # See which JSON files are available
        self.json_files = os.listdir(self.RAW_DATA_PATH)
        self.location = location
        self.filter_json_files()

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
            file for file in self.json_files if self.location.lower() in file.lower()
        ]

        self.user_files = [
            file
            for file in self.json_files
            if "user" in file.lower() and self.location.lower() in file.lower()
        ]

        self.list_files = [
            file
            for file in self.json_files
            if "list" in file.lower() and self.location.lower() in file.lower()
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

        # Convert the JSON data into a DataFrame

        # If nested list

        if isinstance(data[0], list):
            df = pd.DataFrame(data[0])

        else:
            df = pd.DataFrame(data)

        # # Save the DataFrame as a CSV file
        # df.to_csv(output_file, index=False)

        return df

    def concat_dataframes(self, files):
        """
        Reads the JSON files, creates a dataframe
        for each file and concatenates all the dataframes.

        Args:
            files_list (list): List of JSON files.

        Returns:
            DataFrame: The concatenated DataFrame.
        """

        df = pd.DataFrame()

        for file in files:
            input_file = self.RAW_DATA_PATH / file
            input_df = self.convert_to_df(input_file)

            if "relevant" or "content_is_relevant" in input_df.columns:
                df = pd.concat([df, self.convert_to_df(input_file)], ignore_index=True)

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
            user_df = self.concat_dataframes(self.user_files)
            user_df.dropna(subset=["content_is_relevant"], inplace=True)
            # Drop columns that are not needed
            user_df.to_csv(
                self.CLEAN_DATA_PATH / f"{self.location}_user_data.csv",
                index=False,
                encoding="utf-8-sig",
            )
            print(f"User data saved successfully for {self.location}")

        else:
            print(f"No user data found for {self.location}")

        if self.list_files:
            list_df = self.concat_dataframes(self.list_files)
            list_df.dropna(subset=["relevant"], inplace=True)
            list_df.to_csv(
                self.CLEAN_DATA_PATH / f"{self.location}_list_data.csv",
                index=False,
                encoding="utf-8-sig",
            )
            print(f"List data saved successfully for {self.location}")

        else:
            print(f"No list data found for {self.location}")
