"""
This script does the initial filtering process for the twitter
lists
"""

# Global imports

import json
import pandas as pd

from twitter_filtering.utils.constants import PATH, LISTS_KEYWORDS, COLS_TO_KEEP

# Constants


# Functions and pipeline


def read_json(file_path):
    """
    Reads JSON file and returns a dictionary

    Args:
        file_path (str): Path to the JSON file
    Returns:
        data (dict): Dictionary with the JSON data
    """
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


twitter_lists = read_json(
    "C:/Users/fdmol/Desktop/Energy-Lab/twitter_search/twitter_search/data/raw_data/Mumbai_lists.json"
)


print(twitter_lists[1])


class ListFilter:
    """
    Class to filter lists based on keywords
    """

    def __init__(self, df) -> None:
        self.df = df

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
