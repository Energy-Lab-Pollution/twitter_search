"""
This script runs the Twitter search, data collection and filtering process.
"""

# General imports

from pathlib import Path

from config_utils.cities import ALIAS_DICT, CITIES, PILOT_CITIES
from config_utils.queries import QUERIES_EN
from etl.data_collection.get_extra_tweets import TweetGetter
from etl.data_collection.search_users import UserSearcher
from etl.data_collection.tweet_processor import TweetProcessor
from etl.query import Query
from twitter_filtering.users_filtering.filter_users import UserFilter


class TwitterDataHandler:
    """
    This class handles the Twitter search and data collection process.
    """

    QUERIES = QUERIES_EN
    CITIES = CITIES

    def __init__(
        self,
        location,
        account_type,
        num_iterations=1,
    ):
        self.location = location.lower()
        self.account_type = account_type
        self.num_iterations = num_iterations
        self.base_dir = Path(__file__).parent.parent / "data/raw_data"

    def run_all_locations_accounts(self, skip_media=False):
        """
        Runs the entire process for all the available locations
        and cities

        Args
        ----------
            skip_media: str
            Determines if we should skip the search for media accounts
            (there are tons of them)

        """
        for city in CITIES:
            print(f" =============== CITY: {city} ======================")
            self.location = city
            self.run_all_account_types(skip_media)

    def run_pilot_locations_accounts(self, skip_media=False):
        """
        Runs the entire process for all the available locations
        and cities

        Args
        ----------
            skip_media: str
            Determines if we should skip the search for media accounts
            (there are tons of them)

        """
        for city in PILOT_CITIES:
            print(f" =============== CITY: {city} ======================")
            self.location = city
            self.run_all_account_types(skip_media)

    def run_all_account_types(self, skip_media=False):
        """
        Runs the entire process for all the available
        account types for a particular location.

        Args
        ----------
            skip_media: str
            Determines if we should skip the search for media accounts
            (there are tons of them)

        """
        account_types = self.QUERIES
        if skip_media:
            if "media" in account_types:
                del account_types["media"]
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
            / f"{file_city}_{self.account_type}_users_test.json",
            "output_file_tweets": self.base_dir
            / f"{file_city}_{self.account_type}_tweets_test.json",
            "output_file_processing": self.base_dir
            / f"{file_city}_{self.account_type}_processed_users.json",
            "output_file_filter": self.base_dir
            / f"{file_city}_{self.account_type}_users_filtered_{count}.json",
            "output_file_tweet_add": self.base_dir
            / f"{file_city}_{self.account_type}_users_tweet_added",
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
                / f"{file_city}_{self.account_type}_totalusers_{count - 1}.json"
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

        if count == 1:
            self.perform_initial_search()
        # TODO: Add a check to see if we need to get extra tweets
        # self.get_extra_tweets()

        self.filter_users()

        if not hasattr(self.user_filter, "filtered_users"):
            print("No relevant users were found.")
            return

        if not self.user_filter.filtered_users:
            print("No relevant users were found.")
            return

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
            self.paths["output_file_users"],
            self.paths["output_file_tweets"],
            query.text,
        )
        user_searcher.run_search_all()
        if not user_searcher.total_users:
            print("No users found.")
            return
        processor = TweetProcessor(
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
            self.paths["output_file_processing"],
            self.paths["output_file_tweet_add"],
        )
        self.tweet_getter.get_users_tweets()

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

    def reclassify_users(self):
        """
        Reclassify all twitter users if needed, without
        performing any initial search
        """
        COUNT = 1
        self.setup_file_paths(COUNT)
        print("Reclassifying Twitter users based on location...")
        self.user_filter = UserFilter(
            self.paths["input_file_filter"],
            self.paths["output_file_filter"],
        )
        self.user_filter.reclassify_all_users()

    def reclassify_all_locations_accounts(self):
        """
        Runs the entire process for all the available locations
        and cities
        """
        for city in CITIES:
            print(f" =============== CITY: {city} ======================")
            self.location = city
            self.reclassify_all_accounts()

    def reclassify_all_accounts(self):
        """
        Performs the re-classification process for all accounts
        """
        account_types = self.QUERIES
        for account_type in account_types:
            print(
                f" =============== PROCESSING: {account_type} ======================"
            )
            self.account_type = account_type
            self.reclassify_users()
