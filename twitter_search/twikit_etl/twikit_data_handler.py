"""
Script that runs the Twitter Search Pipeline by using Twikit
"""

from pathlib import Path

from config_utils.cities import ALIAS_DICT, CITIES, PILOT_CITIES
from config_utils.queries import QUERIES
from etl.query import Query
from twikit_etl.data_collection.search_twikit_users import TwikitUserSearcher
from twitter_filtering.users_filtering.filter_users import UserFilter


class TwikitDataHandler:
    """
    Class that handles the Twikit search and data collection process
    """

    QUERIES = QUERIES
    CITIES = CITIES
    PILOT_CITIES = PILOT_CITIES

    def __init__(self, location, account_type, num_iterations=1):
        self.location = location.lower()
        self.account_type = account_type
        self.num_iterations = num_iterations
        self.base_dir = Path(__file__).parent.parent / "data/raw_data"

    def run(self):
        """
        Runs the entire process
        """
        for count in range(1, self.num_iterations + 1):
            self.process_iteration(count)
            if count == self.num_iterations:
                break

    def setup_file_paths(self):
        """
        Set up file paths for the current iteration.

        Args:
            count (int): The current iteration count.

        We check if the current location is an alias of a main city
        and set the files accordingly
        """

        print("Checking if city is in secondary cities dictionary")
        if self.location in ALIAS_DICT:
            print(f"{self.location} found in alias dict")
            file_city = ALIAS_DICT[self.location]
        else:
            file_city = self.location

        self.paths = {
            "output_file_users": self.base_dir
            / f"{file_city}_{self.account_type}_users.json",
            "output_file_tweets": self.base_dir
            / f"{file_city}_{self.account_type}_tweets.json",
            "output_file_processing": self.base_dir
            / f"{file_city}_{self.account_type}_processed_users.json",
            "output_file_filter": self.base_dir
            / f"{file_city}_{self.account_type}_users_filtered.json",
            "output_file_tweet_add": self.base_dir
            / f"{file_city}_{self.account_type}_users_tweet_added",
        }

        input_file_processing = (
            self.paths["output_file_tweets"],
            self.paths["output_file_users"],
        )

        self.paths["input_file_processing"] = input_file_processing

        output_file_processing = self.paths["output_file_processing"]
        self.paths["input_file_filter"] = output_file_processing

    def perform_initial_search(self):
        """
        This function runs the initial search for Twitter users, and
        it is only done in the first iteration.
        """
        print("Searching for Twitter users...")
        query = Query(self.location, self.account_type)
        query.query_builder()

        user_searcher = TwikitUserSearcher(
            self.paths["output_file_users"],
            self.paths["output_file_tweets"],
            query.text,
        )
        user_searcher.run_search()
        if not user_searcher.users_list:
            print("No users found.")
            return

        print(query.text)

    def filter_users(self):
        """
        Filter Twitter users based on content relevance.
        """
        print("Filtering Twitter users based on location...")
        self.user_filter = UserFilter(
            self.paths["input_file_filter"],
            self.paths["output_file_filter"],
        )
        self.user_filter.run_filtering()
