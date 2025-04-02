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
        "edge_type", type=int, help="Edge type to choose",
        choices=["retweeters", "followers"]
    )

    args = parser.parse_args()

    if args.w

    network_handler = NetworkHandler(args.location, args.num_users)
