"""
This script runs the Twitter search, data collection and filtering process.
"""

# General imports

from pathlib import Path

from config_utils.queries import QUERIES
from etl.data_collection.get_extra_tweets import TweetGetter
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


class TwitterDataHandler:
    """
    This class handles the Twitter search and data collection process.
    """

    QUERIES = QUERIES

    def __init__(
        self,
        location,
        account_type,
        list_needed,
        num_iterations=1,
    ):
        self.location = location.lower()
        self.account_type = account_type
        self.list_needed = True if list_needed == "True" else False
        self.num_iterations = num_iterations
        self.base_dir = Path(__file__).parent.parent / "data/raw_data"

    def run_all_account_types(self):
        """
        Runs the entire process for all the available
        account types for a particular location.
        """
        account_types = self.QUERIES
        for account_type in account_types:
            print(
                f" =============== PROCESSING: {account_type} ======================"
            )
            self.account_type = account_type
            self.run()

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
        """
        self.paths = {
            "output_file_users": self.base_dir
            / f"{self.location}_{self.account_type}_users_test.json",
            "output_file_tweets": self.base_dir
            / f"{self.location}_{self.account_type}_tweets_test.json",
            "output_file_processing": self.base_dir
            / f"{self.location}_{self.account_type}_processed_users.json",
            "output_file_filter": self.base_dir
            / f"{self.location}_{self.account_type}_users_filtered_{count}.json",
            "output_file_tweet_add": self.base_dir
            / f"{self.location}_{self.account_type}_users_tweet_added",
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

        input_file_processing = (
            self.paths["output_file_tweets"],
            self.paths["output_file_users"],
        )

        self.paths["input_file_processing"] = input_file_processing

        if count == 1:
            output_file_processing = self.paths["output_file_processing"]
            self.paths["input_file_filter"] = output_file_processing

        else:
            self.paths["input_file_filter"] = (
                self.base_dir
                / f"{self.location}_{self.account_type}_totalusers_{count - 1}.json"
            )

    def process_iteration(self, count):
        """
        Process an iteration of the Twitter search and data collection process.

        Args:
            count (int): The current iteration count.

        Returns:
            None
        """

        print(f"Iteration {count}:")
        self.setup_file_paths(count)

        # if count == 1:
        #     self.perform_initial_search()
        # TODO: Add a check to see if we need to get extra tweets
        # self.get_extra_tweets()

        self.filter_users()

        if not self.list_needed:
            print("Lists not needed, exiting.")
            return

        if not self.user_filter.filtered_user:
            print("No relevant users were found.")
            return

        self.handle_lists()

    def perform_initial_search(self):
        """
        This function runs the initial search for Twitter users, and
        it is only done in the first iteration.
        """
        print("Searching for Twitter users...")
        query = Query(self.location, self.account_type)
        query.query_builder()

        print(query.text)

        user_searcher = UserSearcher(
            self.location,
            self.paths["output_file_users"],
            self.paths["output_file_tweets"],
            query.text,
        )
        user_searcher.run_search_all()
        if not user_searcher.total_users:
            print("No users found.")
            return
        processor = TweetProcessor(
            self.location,
            self.account_type,
            self.paths["input_file_processing"],
            self.paths["output_file_processing"],
        )
        processor.run_processing()

    def get_extra_tweets(self):
        """
        In the first iteration, gets extra tweets from any users that
        we deemed relevant.
        """
        self.tweet_getter = TweetGetter(
            self.location,
            self.paths["output_file_processing"],
            self.paths["output_file_tweet_add"],
        )
        self.tweet_getter.get_users_tweets()

    def filter_users(self):
        """
        Filter Twitter users based on location and
        relevance.
        """
        print("Filtering Twitter users based on location...")
        self.user_filter = UserFilter(
            self.location,
            self.paths["input_file_filter"],
            self.paths["output_file_filter"],
        )
        self.user_filter.run_filtering()

    def handle_lists(self):
        """
        Handle the lists associated with the filtered users.
        """
        print("Retrieving lists associated with filtered users...")
        list_getter = ListGetter(
            self.location,
            self.paths["input_file_lists"],
            self.paths["output_file_lists"],
        )
        list_getter.get_lists()

        print("Filtering lists...")
        self.filter_twitter_lists()

        print("Retrieving user data from lists...")
        user_getter = UserGetter(
            self.location,
            self.paths["output_file_filter_lists"],
            self.paths["output_file_total"],
        )
        user_getter.get_users()

    def filter_twitter_lists(self):
        """
        Filter the lists based on some pre-defined keywords.
        """
        list_reader = ListReader(self.paths["input_file_filter_lists"])
        lists_df = list_reader.create_df()
        list_filter = ListFilter(
            lists_df, self.paths["output_file_filter_lists"]
        )
        list_filter.keep_relevant_lists()
