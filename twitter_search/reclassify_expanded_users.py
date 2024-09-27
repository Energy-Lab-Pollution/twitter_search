"""
Main function to run the List Expansion collection process.
"""

from argparse import ArgumentParser

from config_utils.cities import CITIES

# Local imports
from etl.data_cleaning.csv_converter import CSVConverter
from etl.lists_handler import ListsHandler


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

    args = parser.parse_args()
    location = args.location
    account_type = args.account_type
    lists_handler = ListsHandler(location, account_type)

    if args.account_type == "all":
        if args.location == "all":
            lists_handler.reclassify_all_locations_accounts()
            for city in CITIES:
                csv_converter = CSVConverter(city)
                csv_converter.run()

        else:
            lists_handler.reclassify_all_accounts()
            csv_converter = CSVConverter(args.location)
            csv_converter.run()
    else:
        lists_handler.reclassify_users()


if __name__ == "__main__":
    main()
