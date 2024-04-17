"""
This script runs the Twitter search, data collection and filtering process.
"""

# General imports

from pathlib import Path

from etl.data_cleaning.clean_users import UserCleaner
from etl.data_collection.get_lists import ListGetter
from etl.data_collection.get_users import UserGetter
from etl.data_collection.search_users import UserSearcher
from twitter_filtering.lists_filtering.filter_lists import ListFilter, ListReader
from twitter_filtering.users_filtering.users import UserFilter

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
    return count <= num_iterations  # Example: Perform 2 iterations


# Main function -- May need to be a class later


def run_search_twitter(query, location, num_iterations=2):
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

    while True:
        # Set up file paths with count
        dir = Path(__file__).parent.parent / "data/raw_data"
        output_file_search = dir / f"{location}_users_test.json"

        if count == 1:
            input_file_filter = output_file_search
        else:
            input_file_filter = dir / f"{location}_totalusers_{count-1}.json"

        output_file_filter = dir / f"{location}_users_filtered_{count}.json"

        input_file_lists = output_file_filter
        output_file_lists = dir / f"{location}_lists_{count}.json"

        input_file_filter_lists = output_file_lists
        output_file_filter_lists = dir / f"{location}_lists_filtered_{count}.json"

        input_file_total = output_file_filter_lists
        output_file_total = dir / f"{location}_totalusers_{count}.json"

        print(f"Iteration {count}:")

        if count == 1:
            # Perform search only in the first iteration
            print("Searching for Twitter users...")

            user_searcher = UserSearcher(location, output_file_search, query)
            user_searcher.run_search_all()

        # Filter users based on location
        print("Filtering Twitter users based on location...")

        user_filter = UserFilter(location, input_file_filter, output_file_filter)
        user_filter.run_filtering()

        # Retrieve lists associated with filtered users
        print("Retrieving lists associated with filtered users...")
        list_getter = ListGetter(location, input_file_lists, output_file_lists)
        list_getter.get_lists()

        # Filter lists
        filter_twitter_lists(input_file_filter_lists, output_file_filter_lists)

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

    return "Data collection and cleaning process completed."
