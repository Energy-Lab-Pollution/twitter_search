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


# Function to store the data in a CSV file


def convert_jsons_to_csv(location):
    """
    Convert JSON files to CSV files.
    """
    location = location.lower()
    converter = CSVConverter(location)
    converter.run()


# Main function -- May need to be a class later


class TwitterDataHandler:

    def __init__(self, location, account_type, list_needed, num_iterations=2):
        self.location = location.lower()
        self.account_type = account_type
        self.list_needed = list_needed
        self.num_iterations = num_iterations
        self.base_dir = Path(__file__).parent.parent / "data/raw_data"

    def additional_iterations_needed(self, count):
        return count < self.num_iterations

    def setup_file_paths(self, count):
        paths = {
            "output_file_users": self.base_dir
            / f"{self.location}_{self.account_type}_users_test.json",
            "output_file_tweets": self.base_dir
            / f"{self.location}_{self.account_type}_tweets_test.json",
            "output_file_processing": self.base_dir
            / f"{self.location}_{self.account_type}_processed_users.json",
            "input_file_filter": self.base_dir
            / f"{self.location}_{self.account_type}_users_filtered_{count}.json",
            "output_file_filter": self.base_dir
            / f"{self.location}_{self.account_type}_users_filtered_{count}.json",
            "input_file_lists": self.base_dir
            / f"{self.location}_{self.account_type}_lists_{count}.json",
            "output_file_lists": self.base_dir
            / f"{self.location}_{self.account_type}_lists_{count}.json",
            "input_file_filter_lists": self.base_dir
            / f"{self.location}_{self.account_type}_lists_filtered_{count}.json",
            "output_file_filter_lists": self.base_dir
            / f"{self.location}_{self.account_type}_lists_filtered_{count}.json",
            "output_file_total": self.base_dir
            / f"{self.location}_{self.account_type}_totalusers_{count}.json",
        }
        return paths

    def process_iteration(self, count):
        print(f"Iteration {count}:")
        paths = self.setup_file_paths(count)

        if count == 1:
            self.perform_initial_search(
                paths["output_file_users"],
                paths["output_file_tweets"],
                paths["output_file_processing"],
            )

        self.filter_users(
            paths["input_file_filter"], paths["output_file_filter"]
        )

        if not self.list_needed:
            print("Lists not needed, exiting.")
            return

        if not self.user_filter.filtered_user:
            print("No relevant users were found.")
            return

        self.handle_lists(
            paths["input_file_lists"],
            paths["output_file_lists"],
            paths["input_file_filter_lists"],
            paths["output_file_filter_lists"],
            paths["output_file_total"],
        )

    def perform_initial_search(
        self, output_file_users, output_file_tweets, output_file_processing
    ):
        print("Searching for Twitter users...")
        query = Query(self.location, self.account_type)
        query.query_builder()
        user_searcher = UserSearcher(
            self.location, output_file_users, output_file_tweets, query.text
        )
        user_searcher.run_search_all()
        processor = TweetProcessor(
            self.location,
            (output_file_tweets, output_file_users),
            output_file_processing,
        )
        processor.run_processing()

    def filter_users(self, input_file, output_file):
        print("Filtering Twitter users based on location...")
        self.user_filter = UserFilter(self.location, input_file, output_file)
        self.user_filter.run_filtering()

    def handle_lists(
        self,
        input_file_lists,
        output_file_lists,
        input_file_filter_lists,
        output_file_filter_lists,
        output_file_total,
    ):
        print("Retrieving lists associated with filtered users...")
        list_getter = ListGetter(
            self.location, input_file_lists, output_file_lists
        )
        list_getter.get_lists()

        print("Filtering lists...")
        self.filter_twitter_lists(
            input_file_filter_lists, output_file_filter_lists
        )

        print("Retrieving user data from lists...")
        user_getter = UserGetter(
            self.location, output_file_filter_lists, output_file_total
        )
        user_getter.get_users()

    def filter_twitter_lists(self, input_file, output_file):
        list_reader = ListReader(input_file)
        lists_df = list_reader.create_df()
        list_filter = ListFilter(lists_df, output_file)
        list_filter.keep_relevant_lists()

    def run_search_twitter(
        location, account_type, list_needed, num_iterations=2
    ):
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
            output_file_users = (
                dir / f"{location}_{account_type}_users_test.json"
            )
            output_file_tweets = (
                dir / f"{location}_{account_type}_tweets_test.json"
            )

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
            list_getter = ListGetter(
                location, input_file_lists, output_file_lists
            )
            list_getter.get_lists()
            print("Output file:", output_file_lists)

            # Filter lists
            print("Filtering lists...")
            print("Input file:", input_file_filter_lists)
            filter_twitter_lists(
                input_file_filter_lists, output_file_filter_lists
            )
            print("Output file:", output_file_filter_lists)

            # Retrieve user data from the retrieved lists
            print("Retrieving user data from lists...")
            user_getter = UserGetter(
                location, input_file_total, output_file_total
            )
            user_getter.get_users()

            # Increment count for the next iteration
            count += 1

            # Check if additional iterations are needed
            if additional_iterations_needed(count, num_iterations):
                continue
            else:
                break
