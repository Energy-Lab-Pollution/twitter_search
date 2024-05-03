"""
Main function to run the Twitter search and data collection process.
"""

from etl import run_search_twitter
from argparse import ArgumentParser
from distutils.util import strtobool


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
              [media,organizations,policymaker,politicians,researcher,environment]",
        choices=[
            "media",
            "organizations",
            "policymaker",
            "politicians",
            "researcher",
            "environment",
        ],
    )

    parser.add_argument(
        "list_needed",
        type=str,
        help="Specify if you need list based expansions.",
        choices=["True", "False"],
    )

    parser.add_argument(
        "--num_iterations",
        type=int,
        help="Specify the number of iterations to run.",
    )

    args = parser.parse_args()
    location = args.location
    account_type = args.account_type
    list_needed = strtobool(args.list_needed)
    print(list_needed, "list needed?")

    print("Building query...")

    if args.num_iterations:
        print(
            f"""Running search with {args.num_iterations} iterations for {location}
            and {account_type} account type"""
        )
        num_iterations = args.num_iterations
        run_search_twitter.run_search_twitter(
            location, account_type, list_needed, num_iterations
        )

    else:
        run_search_twitter.run_search_twitter(
            location, account_type, list_needed
        )
