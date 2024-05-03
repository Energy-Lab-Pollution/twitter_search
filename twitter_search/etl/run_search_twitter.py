"""
This script runs the Twitter search, data collection and filtering process.
"""

# General imports

from pathlib import Path

from etl.data_cleaning.create_csvs import CSVConverter
from etl.data_collection.get_lists import ListGetter
from etl.data_collection.get_users import UserGetter
from etl.data_collection.search_users import UserSearcher
from etl.data_collection.tweet_processor import TweetProcessor
from etl.query import Query
from twitter_filtering.lists_filtering.filter_lists import (
    ListFilter,
    ListReader,
)
from twitter_filtering.users_filtering.filter_users import UserFilter

# Utils functions


def filter_twitter_lists(input_file_filter, output_file_filter):
    """
    This helper function will filter the lists based on some pre-defined
    keywords.

    Args:
        input_file_filter (str): The input file containing the lists.
        output_file_filter (str): The output file to save the filtered lists.

    Returns:
        None
    """
    list_reader = ListReader(input_file_filter)
    lists_df = list_reader.create_df()

    print("Filtering dataframe for relevant lists")
    list_filter = ListFilter(lists_df, output_file_filter)
    list_filter.keep_relevant_lists()


def additional_iterations_needed(count, num_iterations=2):
    """
    Determine if additional iterations are needed based on the count.
    """
    return count <= num_iterations


# Main function -- May need to be a class later


def run_search_twitter(location, account_type, list_needed, num_iterations=2):
    """
    Run Twitter search and data collection process.

    Args:
        query (str): The search query to search for Twitter users.
        location (str): The location to filter the search for Twitter users.

    Returns:
        str: A message indicating the completion of the data cleaning process.

    Raises:
        ValueError: If any of the input arguments are invalid.
    """
    count = 1
    location = location.lower()
    query = Query(location, account_type)
    query.query_builder()

    while True:
        # Set up file paths with count
        dir = Path(__file__).parent.parent / "data/raw_data"
        output_file_users = dir / f"{location}_{account_type}_users_test.json"
        output_file_tweets = dir / f"{location}_{account_type}_tweets_test.json"

        input_file_processing = (output_file_tweets, output_file_users)
        output_file_processing = (
            dir / f"{location}_{account_type}_processed_users.json"
        )

        if count == 1:
            input_file_filter = output_file_processing
        else:
            input_file_filter = (
                dir / f"{location}_{account_type}_totalusers_{count-1}.json"
            )

        output_file_filter = (
            dir / f"{location}_{account_type}_users_filtered_{count}.json"
        )

        input_file_lists = output_file_filter
        output_file_lists = (
            dir / f"{location}_{account_type}_lists_{count}.json"
        )

        input_file_filter_lists = output_file_lists
        output_file_filter_lists = (
            dir / f"{location}_{account_type}_lists_filtered_{count}.json"
        )

        input_file_total = output_file_filter_lists
        output_file_total = (
            dir / f"{location}_{account_type}_totalusers_{count}.json"
        )

        print(f"Iteration {count}:")

        if count == 1:
            # Perform search only in the first iteration
            print("Searching for Twitter users...")

            user_searcher = UserSearcher(
                location, output_file_users, output_file_tweets, query.text
            )
            user_searcher.run_search_all()
            processor = TweetProcessor(
                location, input_file_processing, output_file_processing
            )
            processor.run_processing()

        # Filter users based on location
        print("Filtering Twitter users based on location...")

        print("Input file:", input_file_filter)

        user_filter = UserFilter(
            location, input_file_filter, output_file_filter
        )
        user_filter.run_filtering()

        if not list_needed:
            print("lists not needed, exiting.")
            break

        if not user_filter.filtered_user:
            print("No relevant users were found.")
            break

        print("Lists - input file:", input_file_lists)
        # Retrieve lists associated with filtered users
        print("Retrieving lists associated with filtered users...")
        list_getter = ListGetter(location, input_file_lists, output_file_lists)
        list_getter.get_lists()
        print("Output file:", output_file_lists)

        # Filter lists
        print("Filtering lists...")
        print("Input file:", input_file_filter_lists)
        filter_twitter_lists(input_file_filter_lists, output_file_filter_lists)
        print("Output file:", output_file_filter_lists)

        # Retrieve user data from the retrieved lists
        print("Retrieving user data from lists...")
        user_getter = UserGetter(location, input_file_total, output_file_total)
        user_getter.get_users()

        # Increment count for the next iteration
        count += 1

        # Check if additional iterations are needed
        if additional_iterations_needed(count, num_iterations):
            continue
        else:
            break

    print("Saving users and lists as CSV files...")
    csv_converter = CSVConverter(location)
    csv_converter.run()
