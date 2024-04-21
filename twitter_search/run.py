"""
Main function to run the Twitter search and data collection process.
"""

from etl import run_search_twitter
from argparse import ArgumentParser


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
        help="type of accouts that you want\
              [media,organizations,policymaker,politicians,researcher,environment]",
    )

    parser.add_argument(
        "--num_iterations",
        type=int,
        help="Specify the number of iterations to run.",
    )

    # parser.add_argument("--algorithm", type=int, choices=[1, 2], \
    # help="Specify the algorithm (1 or 2).")
    args = parser.parse_args()
    location = args.location
    account_type = args.account_type
    print("Building query...")


    if args.num_iterations:
        num_iterations = args.num_iterations
        run_search_twitter.run_search_twitter(location, account_type,num_iterations)

    else:
        run_search_twitter.run_search_twitter(location,account_type)
