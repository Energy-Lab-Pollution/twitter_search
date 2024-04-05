"""
Script that executes the main pipelines for user and lists filtering
"""

# Global imports
import argparse

# Local imports
from lists_filtering.filter_lists import ListReader, ListFilter
from utils.constants import CLEAN_DATA_PATH


parser = argparse.ArgumentParser(description="Apply list filters to a given file")
parser.add_argument("filename", type=str, help="Filename to parse")

args = parser.parse_args()

if __name__ == "__main__":

    if ".json" not in args.filename:
        raise ValueError("File must be of JSON type")

    print("Reading JSON to create Lists dataframe...")
    print(f"Reading {args.filename}")
    list_reader = ListReader(args.filename)
    lists_df = list_reader.create_df()

    print("Filtering dataframe for relevant lists")
    list_filter = ListFilter(lists_df)
    relevant_lists = list_filter.keep_relevant_lists()

    new_filename = args.filename.replace(".json", "_filtered.json")
    new_path = f"{CLEAN_DATA_PATH}/{new_filename}"
    relevant_lists.to_json(f"{new_path}", orient="records")