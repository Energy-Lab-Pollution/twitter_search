"""
Script that runs the Twitter Search Pipeline by using Twikit
"""
import os
from datetime import datetime
from pathlib import Path

import pytz
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
        self.base_dir = Path(__file__).parent.parent / "data/twikit_raw_data"

        self.todays_date = datetime.now(pytz.timezone("America/Chicago"))
        self.todays_date_str = datetime.strftime(self.todays_date, "%Y-%m-%d")

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

        # Will create a new folder per day
        self.date_dir = f"{self.base_dir}/{self.todays_date_str}"

        if os.path.exists(self.date_dir):
            os.makedirs(self.date_dir)

        else:
            print("Dir already exists")

        self.paths = {
            "output_file_users": self.date_dir
            / f"{file_city}_{self.account_type}_users.json",
            "output_file_tweets": self.date_dir
            / f"{file_city}_{self.account_type}_tweets.json",
            "output_file_processing": self.date_dir
            / f"{file_city}_{self.account_type}_processed_users.json",
            "output_file_filter": self.date_dir
            / f"{file_city}_{self.account_type}_users_filtered.json",
            "output_file_tweet_add": self.date_dir
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

    def run_iteration(self):
        """
        Process an iteration of the Twitter search and data collection process.
        """

        self.setup_file_paths()
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
            self.run_iteration()


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