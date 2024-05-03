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


# Main function -- May need to be a class later
class TwitterDataHandler:
    """
    This class handles the Twitter search and data collection process.
    """

    def __init__(
        self,
        location,
        account_type,
        list_needed,
        num_iterations=2,
        convert_to_csv=False,
    ):
        self.location = location.lower()
        self.account_type = account_type
        self.list_needed = list_needed
        self.num_iterations = num_iterations
        self.base_dir = Path(__file__).parent.parent / "data/raw_data"
        self.convert_to_csv = convert_to_csv

    def run(self):
        """
        Runs the entire process
        """
        for count in range(1, self.num_iterations + 1):
            self.process_iteration(count)
            if count == self.num_iterations:
                break

    def setup_file_paths(self, count):
        """
        Set up file paths for the current iteration.

        Args:
            count (int): The current iteration count.

        Returns:
            dict: A dictionary containing the file paths for the current iteration.
        """
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

    def convert_jsons_to_csv(self):
        """
        Converts JSON files to CSV files.

        Args:
            location (str): The location to filter on.

        Returns:
            None
        """
        converter = CSVConverter(self.location)
        converter.run()

    def process_iteration(self, count):
        """
        Process an iteration of the Twitter search and data collection process.

        Args:
            count (int): The current iteration count.

        Returns:
            None
        """

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
        """
        This function runs the initial search for Twitter users, and
        it is only done in the first iteration.

        Args:
            output_file_users (str): The output file to save the Twitter users.
            output_file_tweets (str): The output file to save the tweets.

        Returns:
            None
        """
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
        """
        Filter Twitter users based on location and
        relevance.

        Args:
            input_file (str): The input file containing the Twitter users.
            output_file (str): The output file to save the filtered users.

        Returns:
            None
        """
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
        """
        Handle the lists associated with the filtered users.

        Args:
            input_file_lists (str): The input file containing the lists.
            output_file_lists (str): The output file to save the lists.
            input_file_filter_lists (str): The input file containing the filtered lists.
            output_file_filter_lists (str): The output file to save the filtered lists.
            output_file_total (str): The output file to save the total users.

        Returns:
            None
        """
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
        """
        Filter the lists based on some pre-defined keywords.

        Args:
            input_file (str): The input file containing the lists.
            output_file (str): The output file to save the filtered lists.

        Returns:
            None

        """
        list_reader = ListReader(input_file)
        lists_df = list_reader.create_df()
        list_filter = ListFilter(lists_df, output_file)
        list_filter.keep_relevant_lists()
