"""
Main function to run the Twitter search and data collection process.
"""

from twitter_search.etl import run_search_twitter
from argparse import ArgumentParser


def build_query(location):

    return f"(air pollution {location} OR {location} air OR {location} \
        pollution OR {location} public health OR bad air {location} OR \
        {location} asthma OR {location} polluted OR pollution control board) \
        (#pollution OR #environment OR #cleanair OR #airquality) -is:retweet"


def main():

    parser = ArgumentParser(
        description="Get users from Twitter \
                        based on location and algorithm."
    )
    parser.add_argument(
        "-location",
        type=str,
        help="Specify the\
                            location (city) for Twitter user search.",
    )
    # parser.add_argument("--algorithm", type=int, choices=[1, 2], \
    # help="Specify the algorithm (1 or 2).")
    args = parser.parse_args()
    try:
        location = args.location
        print("Building query...")
        # Build the query based on args.location
        query = build_query(location)
        print(query)
        run_search_twitter.run_search_twitter(query, location)

    except Exception as e:
        print(f"Error running process: {e}")
