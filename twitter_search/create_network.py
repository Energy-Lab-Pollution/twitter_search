"""
Script to get followers and retweeters from a particular set
of X Users
"""

import asyncio
import time
from argparse import ArgumentParser

from config_utils.constants import FIFTEEN_MINUTES
from network.network_handler import NetworkHandler


if __name__ == "__main__":
    #TODO: Add dash-dash to avoid order and be more flexible
    parser = ArgumentParser(
        "Parameters to get users data to generate a network"
    )
    parser.add_argument(
        "location", type=str, help="Location to read users from"
    )
    parser.add_argument(
        "extraction_type",
        type=str,
        choices=["twikit", "x"],
        help="Choose how to get users",
    )
    parser.add_argument(
        "wait",
        type=str,
        choices=["Yes", "No"],
        help="Decide whether to wait 15 mins or not",
    )
    parser.add_argument(
        "file_flag",
        type=str,
        choices=["Yes", "No"],
        help="Determines if root users will be extracted from the .csv file",
    )
    parser.add_argument(
        "account_number",
        type=int,
        help="Account number to use with twikit",
    )
    parser.add_argument(
        "--ascending",
        type=str,
        choices=["Yes", "No"],
        help="Account number to use with twikit",
    )

    args = parser.parse_args()

    if args.wait == "Yes":
        print("Sleeping for 15 minutes...")
        time.sleep(FIFTEEN_MINUTES)

    file_flag = True if args.file_flag == "Yes" else False
    ascending = True if args.reverse == "Yes" else False
    network_handler = NetworkHandler(args.location)

    asyncio.run(
        network_handler.create_user_network(
            args.extraction_type, args.account_number, file_flag,
            ascending
        )
    )
