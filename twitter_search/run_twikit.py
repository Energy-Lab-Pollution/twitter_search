"""
Main function to run the Twitter search and data collection process
using the Twikit Library
"""

from argparse import ArgumentParser

from config_utils.cities import CITIES, PILOT_CITIES
from config_utils.util import strtobool
from config_utils.constants import TWIKIT_THRESHOLD

# Local imports
from etl.data_cleaning.csv_converter import CSVConverter
from twikit_etl.twikit_data_handler import TwikitDataHandler


def main():
    parser = ArgumentParser(
        description="Get users from Twitter based on location,type and algorithm."
    )

    parser.add_argument(
        "location",
        type=str,
        help="Specify the location (city) for Twitter user search.",
    )

    parser.add_argument(
        "account_type",
        type=str,
        help="type of accounts that you want\
              [media,organizations,policymaker,politicians,researcher,environment,all]",
        choices=[
            "media",
            "organizations",
            "policymaker",
            "politicians",
            "researcher",
            "environment",
            "all",
        ],
    )

    parser.add_argument(
        "--skip_media",
        type=str,
        help="Specify if the media accounts should be skipped",
        choices=["True", "False"],
    )

    args = parser.parse_args()
    location = args.location
    account_type = args.account_type
    print("Building query...")

    twikit_data_handler = TwikitDataHandler(location, account_type)

    if args.skip_media:
        skip_media = strtobool(args.skip_media)

    if args.account_type == "all":
        if args.location == "all":
            if args.skip_media:
                twikit_data_handler.run_all_locations_accounts(skip_media)
            else:
                twikit_data_handler.run_all_locations_accounts()

            # for city in CITIES:
            #     csv_converter = CSVConverter(city)
            #     csv_converter.run()

        elif args.location == "pilot_cities":
            if args.skip_media:
                twikit_data_handler.run_pilot_locations_accounts(skip_media)
            else:
                twikit_data_handler.run_pilot_locations_accounts()

            # for city in PILOT_CITIES:
            #     csv_converter = CSVConverter(city)
            #     csv_converter.run()

        # Since extracting one city, we can use a given number of requests
        else:
            if args.skip_media:
                twikit_data_handler.run_all_account_types(TWIKIT_THRESHOLD, skip_media)
            else:
                twikit_data_handler.run_all_account_types(TWIKIT_THRESHOLD)
            # csv_converter = CSVConverter(args.location)
            # csv_converter.run()
    else:
        twikit_data_handler.run()

if __name__ == "__main__":
    main()