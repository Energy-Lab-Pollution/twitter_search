"""
Script that executes the main pipelines for user and lists filtering
"""

# Global imports
import argparse

# Local imports
from lists_filtering.filter_lists import ListReader, ListFilter


parser = argparse.ArgumentParser(description="Apply list filters to a given file")
parser.add_argument("filename", type=str, help="Filename to parse")

args = parser.parse_args()

if __name__ == "__main__":

    print("Reading JSON to create Lists dataframe...")
    print(f"Reading {args.filename}")
    list_reader = ListReader(args.filename)
    lists_df = list_reader.create_df()

    print("Filtering dataframe for relevant lists")
    list_filter = ListFilter(lists_df)
    lists_df["relevant"] = lists_df.apply(list_filter.is_relevant, axis=1)
    relevant_lists = lists_df.loc[lists_df["relevant"].isin([True]), :]
    print(relevant_lists)
    print("Done!")
