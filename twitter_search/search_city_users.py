"""
Script to get followers and retweeters from a particular set
of X Users
"""

import asyncio
import time
from argparse import ArgumentParser

from config_utils.constants import FIFTEEN_MINUTES
from network.sqs_city_users import CityUsers


if __name__ == "__main__":
    # parameters: [location, tweet_count, keywords (both hashtags, timeperiod and keywords)]
    # call relevant methods on the city user class
    #TODO: Add dash-dash to avoid order and be more flexible

    parser = ArgumentParser(
        "Parameters to get users data to generate a network"
    )
    parser.add_argument(
        "--location", type=str, help="Location to search users from"
    )
    parser.add_argument(
        "--tweet_count", type=str, help="Number of tweets to get"
    )
    parser.add_argument(
        "--extraction_type",
        type=str,
        choices=["twikit", "x"],
        help="Choose how to get users",
    )
    parser.add_argument(
        "--wait",
        type=str,
        choices=["Yes", "No"],
        help="Decide whether to wait 15 mins or not",
    )
    parser.add_argument(
        "--file_flag",
        type=str,
        choices=["Yes", "No"],
        help="Determines if root users will be extracted from the .csv file",
    )
    parser.add_argument(
        "--account_number",
        type=int,
        help="Account number to use with twikit",
    )
    args = parser.parse_args()

    if args.wait == "Yes":
        print("Sleeping for 15 minutes...")
        time.sleep(FIFTEEN_MINUTES)

    file_flag = True if args.file_flag == "Yes" else False
    city_users = CityUsers(args.location)