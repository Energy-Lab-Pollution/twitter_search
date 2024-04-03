"""
This script does the initial filtering process for the twitter
lists
"""

# Global imports

import json

# Constants

RAW_DATA_PATH = (
    "mnt/c/Users/fdmol/Desktop/Energy-Lab/twitter_search/twitter_search/data/raw_data"
)

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
