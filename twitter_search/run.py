"""
Main function to run the Twitter search and data collection process.
"""

from etl.run_search_twitter import TwitterDataHandler
from argparse import ArgumentParser

def strtobool (val):
    """Convert a string representation of truth to true (1) or false (0).
    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return 1
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))

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

    twitter_data_handler = TwitterDataHandler(location, account_type, list_needed)

    if args.num_iterations:
        print(
            f"""Running search with {args.num_iterations} iterations for {location}
            and {account_type} account type"""
        )
        num_iterations = args.num_iterations
        twitter_data_handler.num_iterations = num_iterations

    twitter_data_handler.run()
