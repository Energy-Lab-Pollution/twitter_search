"""
This script converts the JSON files into CSV files for easier data manipulation.
"""

# General imports
import pandas as pd
import json
import os
from pathlib import Path


# Lists_filtering constants


script_path = Path(__file__).resolve()
project_root = script_path.parents[1]


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
            file
            for file in self.json_files
            if self.location.lower() in file.lower()
        ]

    def convert_to_df(self, input_file):
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

    def concat_dataframes(self):
        """
        Reads the JSON files, creates a dataframe
        for each file and concatenates all the dataframes.

        Args:
            files_list (list): List of JSON files.

        Returns:
            DataFrame: The concatenated DataFrame.
        """

        df = pd.DataFrame()

        if not self.filtered_files:
            print(f"No files found for {self.location}")
            return df

        for filtered_file in self.filtered_files:
            input_file = self.RAW_DATA_PATH / filtered_file
            print(f"Converting {input_file} to CSV")
            df = pd.concat(
                [df, self.convert_to_df(input_file)], ignore_index=True
            )

        return df


if __name__ == "__main__":

    # Filter the JSON files based on the location
    location = "Bangalore"
    csv_converter = CSVConverter(location)
