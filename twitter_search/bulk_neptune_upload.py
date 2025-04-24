"""
Script that transform network JSON files to csvs and
uploads them in bulk to Neptune
"""
from argparse import ArgumentParser

from network.neptune_bulk_upload import NeptuneBulkUploader

if __name__ == "__main__":
    # Convert and load retweets

    parser = ArgumentParser(
        "Parameters to bulk upload users from csv files to Neptune"
    )
    parser.add_argument(
        "location", type=str, help="Location to read users from"
    )
    parser.add_argument(
        "graph_type",
        type=str,
        help="Edge type to choose",
        choices=["retweet", "follower"],
    )

    args = parser.parse_args()

    neptune_bulk_uploader = NeptuneBulkUploader(args.location, args.graph_type)
    neptune_bulk_uploader.run()
