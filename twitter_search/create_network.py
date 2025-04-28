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
    parser = ArgumentParser(
        "Parameters to get users data to generate a network"
    )
    parser.add_argument(
        "location", type=str, help="Location to read users from"
    )
    parser.add_argument(
        "extraction_type",
        type=str,
        choices=["file", "twikit", "x"],
        help="Choose how to get users",
    )
    parser.add_argument(
        "wait",
        type=str,
        choices=["Yes", "No"],
        help="Decide whether to wait 15 mins or not",
    )

    args = parser.parse_args()

    if args.wait == "Yes":
        print("Sleeping for 15 minutes...")
        time.sleep(FIFTEEN_MINUTES)

    network_handler = NetworkHandler(args.location)
    asyncio.run(network_handler.create_user_network(extraction_type="file"))
