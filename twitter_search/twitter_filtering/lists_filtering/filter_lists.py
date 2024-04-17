"""
This script does the initial filtering process for the twitter
lists
"""

# Global imports

import json
import pandas as pd

from utils.constants import (
    RAW_DATA_PATH,
    LISTS_KEYWORDS,
    COLS_TO_KEEP,
)

# Constants
FILENAME = "Mumbai_lists"

# Classes and pipeline


class ListReader:

    PATH = RAW_DATA_PATH
    LISTS_KEYWORDS = LISTS_KEYWORDS
    COLS_TO_KEEP = COLS_TO_KEEP

    def __init__(self, list_filename) -> None:
        self.list_filename = list_filename

    def read_json(self):
        """
        Reads JSON file and returns a dictionary

        Args:
            file_path (str): Path to the JSON file
        Returns:
            data (dict): Dictionary with the JSON data
        """

        with open(f"{self.list_filename}", "r") as file:
            data = json.load(file)
        self.twitter_lists = data

    def parse_into_df(self):
        """
        Creates a DataFrame from the JSON data

        Args:
            lists (list): List of dictionaries with the JSON data
        Returns:
            df (pd.DataFrame): DataFrame with the JSON data
        """

        self.lists_df = pd.DataFrame([])

        for twitter_list in self.twitter_lists:
            if not twitter_list:
                # remove entry if empty
                continue

            else:
                # create a dataframe from the list
                list_df = pd.DataFrame(twitter_list)
                self.lists_df = pd.concat([self.lists_df, list_df], ignore_index=True)

        self.lists_df = self.lists_df.loc[:, COLS_TO_KEEP].copy()

    def create_df(self):
        """
        Performs the complete pipeline to get dataframe of lists
        """
        self.read_json()
        self.parse_into_df()
        self.lists_df.drop_duplicates(subset=["list_id"], inplace=True)

        return self.lists_df


class ListFilter:
    """
    Class to filter lists based on keywords
    """

    def __init__(self, df, output_file) -> None:
        self.df = df
        self.output_file = output_file

    @staticmethod
    def clean_text(text):
        """
        Cleans text from special characters

        Args:
            text (str): Text to be cleaned
        Returns:
            text (str): Cleaned text
        """
        # remove special characters
        text = text.lower()

        return text

    @staticmethod
    def filter_text(text):
        """
        Determines if the text contains a keyword
        of interest
        """
        text = text.lower()
        text_list = text.strip().split()

        for keyword in LISTS_KEYWORDS:
            if keyword in text_list:
                return True

        return False

    def is_relevant(self, row):
        """
        Creates an additional column to determine if the
        list is relevant or not
        """

        relevant_name = self.filter_text(row["name"])

        # If name is not relevant, check description
        if not relevant_name:
            # If description is not relevant, return False
            relevant_description = self.filter_text(row["description"])
            return relevant_description

        # If name is relevant, return True
        else:
            return relevant_name

    def keep_relevant_lists(self):
        """
        Filters the relevant lists from the dataframe
        read when initializing the class
        """
        self.df["relevant"] = self.df.apply(self.is_relevant, axis=1)
        self.relevant_lists = self.df.loc[self.df["relevant"].isin([True]), :]
        self.relevant_lists.reset_index(drop=True, inplace=True)

        self.relevant_lists.to_json(f"{self.output_file}", orient="records")

        return self.relevant_lists
