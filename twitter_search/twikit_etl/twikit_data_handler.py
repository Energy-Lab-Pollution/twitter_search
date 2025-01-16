"""
Script that runs the Twitter Search Pipeline by using Twikit
"""
import os
from datetime import datetime
from pathlib import Path

import pytz
from config_utils.cities import ALIAS_DICT, CITIES, PILOT_CITIES
from config_utils.queries import QUERIES
from config_utils.constants import TWIKIT_THRESHOLD
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
    TWIKIT_THRESHOLD = TWIKIT_THRESHOLD

    def __init__(self, location, account_type):
        self.location = location.lower()
        self.account_type = account_type
        self.base_dir = Path(__file__).parent.parent / "data/twikit_raw_data"

        self.todays_date = datetime.now(pytz.timezone("America/Chicago"))
        self.todays_date_str = datetime.strftime(self.todays_date, "%Y-%m-%d")
        self.num_accounts = len(self.QUERIES)

    def get_city_num_requests(self, num_cities):
        """
        Gets each cities' particular num of requests
        
        Twikit can only process 50 requests to get tweets in a 15 min
        interval. Therefore, for several cities, we need to determine
        how many requests each city will get. 

        Args:
            num_cities: int determining the number of cities to be processed
        """
        # Get number of accounts needed per city
        city_requests = self.TWIKIT_THRESHOLD / num_cities
        remainder_requests = self.TWIKIT_THRESHOLD % num_cities

        if city_requests < self.num_accounts:
            print("City requests: Not enough requests to extract all accounts")
            return None
        
        # Create list of num requests per city
        requests_list = []
        for _ in range(0, num_cities):
            city_requests = round(city_requests)
            requests_list.append(city_requests)

        # If remainder exists, add to last city
        if remainder_requests > 0:
            num_requests = requests_list[-1]
            num_requests += remainder_requests
            requests_list[-1] = num_requests

        return requests_list

    def get_account_num_requests(self, city_requests):
        """
        Gets number of requests for each particular account.
        
        Twikit can only process 50 requests to get tweets in a 15 min
        interval. Therefore, for several cities, we need to determine
        how many requests each city will get. 

        Args:
            city_requests: int determining the number of requests per city
        """
        account_requests = city_requests / self.num_accounts
        remainder_requests = city_requests % self.num_accounts

        if account_requests < 1:
            print("Account requests: Not enough requests to extract all accounts")
            return None
        
        # Create list of num requests per account 
        requests_list = []
        for _ in range(0, self.num_accounts):
            # round to nearest int
            account_requests = round(account_requests)
            requests_list.append(account_requests)

        # If remainder exists, add to last account
        if remainder_requests > 0:
            num_requests = requests_list[-1]
            num_requests += remainder_requests
            requests_list[-1] = num_requests

        return requests_list


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
        self.date_dir = self.base_dir / f"{self.todays_date_str}"

        if not os.path.exists(self.date_dir):
            os.makedirs(self.date_dir)

        else:
            print("Dir already exists")

        self.paths = {
            "output_file_users": self.date_dir
            / f"{file_city}_{self.account_type}_users.json",
            "output_file_tweets": self.date_dir
            / f"{file_city}_{self.account_type}_tweets.json",
            "input_file_filter": self.date_dir
            / f"{file_city}_{self.account_type}_users.json",
            "output_file_filter": self.date_dir
            / f"{file_city}_{self.account_type}_users_filtered.json",
        }

    def perform_initial_search(self, threshold):
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
            threshold,
            query.text,
        )

        user_searcher.run_search()
        if not user_searcher.users_list:
            print("No users found.")
            return

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

    def run(self, threshold=None):
        """
        Process an iteration of the Twitter search and 
        data collection process.

        Args:
            threshold: int with the number of requests for the current
                       search
        """

        print(f"Threshold is {threshold}")

        self.setup_file_paths()
        self.perform_initial_search(threshold)
        self.filter_users()

        if not hasattr(self.user_filter, "filtered_users"):
            print("No relevant users were found.")
            return

        if not self.user_filter.filtered_users:
            print("No relevant users were found.")
            return

    def run_all_account_types(self, city_requests, skip_media=False):
        """
        Runs the entire process for all the available
        account types for a particular location.

        Args
        ----------
            city_requests: int with max number of requests for a given city
            skip_media: str
                Determines if we should skip the search for media accounts
                (there are tons of them)

        """
        account_types = self.QUERIES
        self.skip_media = skip_media

        if skip_media:
            if "media" in account_types:
                del account_types["media"]
                # Number of account types 
                self.num_accounts = len(self.QUERIES) - 1
    
        accounts_requests = self.get_account_num_requests(city_requests)
        print(accounts_requests)

        for account_type, account_requests in zip(account_types, accounts_requests):
            print(
                f" =============== PROCESSING: {account_type} ======================"
            )
            self.account_type = account_type
            self.run(account_requests)


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
        # Get available number of requests per city
        cities_requests = self.get_city_num_requests(len(PILOT_CITIES))
        for city, city_requests in zip(PILOT_CITIES, cities_requests):
            print(f" =============== CITY: {city} ======================")
            self.location = city
            self.run_all_account_types(city_requests, skip_media)


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
        # Get available number of requests per city
        cities_requests = self.get_city_num_requests(len(CITIES))
        for city, city_requests in zip(CITIES, cities_requests):
            print(f" =============== CITY: {city} ======================")
            self.location = city
            self.run_all_account_types(skip_media, city_requests)