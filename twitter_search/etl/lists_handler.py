"""
This script runs the Twitter search, data collection and filtering process.
"""

# General imports

from pathlib import Path

from config_utils.cities import CITIES
from config_utils.queries import QUERIES
from etl.data_collection.get_lists import ListGetter
from etl.data_collection.get_users import UserGetter
from twitter_filtering.lists_filtering.filter_lists import (
    ListFilter,
    ListReader,
)
from twitter_filtering.users_filtering.filter_users import UserFilter


class ListsHandler:
    """
    This class handles the Twitter search and data collection process.
    """

    QUERIES = QUERIES
    CITIES = CITIES

    def __init__(
        self,
        location,
        account_type,
    ):
        self.location = location.lower()
        self.account_type = account_type
        self.base_dir = Path(__file__).parent.parent / "data/raw_data"
        self.setup_file_paths()

    def setup_file_paths(self):
        """
        Set up file paths for the current iteration.

        Args:
            count (int): The current iteration count.
        """
        if self.account_type == "manually_added":
            self.paths = {
                "output_file_filter_lists": self.base_dir
                / f"{self.account_type}_lists.json",
                "output_file_total": self.base_dir
                / f"{self.account_type}_expanded_users.json",
                "output_file_filter_total": self.base_dir
                / f"{self.account_type}_expanded_users_filtered.json",
            }

        else:
            self.paths = {
                "input_file_lists": self.base_dir
                / f"{self.location}_{self.account_type}_users_filtered_1.json",
                "output_file_lists": self.base_dir
                / f"{self.location}_{self.account_type}_lists.json",
                "input_file_filter_lists": self.base_dir
                / f"{self.location}_{self.account_type}_lists.json",
                "output_file_filter_lists": self.base_dir
                / f"{self.location}_{self.account_type}_lists_filtered.json",
                "output_file_total": self.base_dir
                / f"{self.location}_{self.account_type}_expanded_users.json",
                "output_file_filter_total": self.base_dir
                / f"{self.location}_{self.account_type}_expanded_users_filtered.json",
            }

    def perform_list_expansion(self):
        """
        Handle the lists associated with the filtered users.
        """
        # Setting up paths
        self.setup_file_paths()

        print("Retrieving lists associated with filtered users...")
        list_getter = ListGetter(
            self.location,
            self.paths["input_file_lists"],
            self.paths["output_file_lists"],
        )
        list_getter.get_lists()

        print("Filtering lists...")
        self.filter_twitter_lists()

        # Only get users if lists were found
        if not self.lists_df.empty:
            print("Retrieving and filtering user data from lists...")
            user_getter = UserGetter(
                self.location,
                self.paths["output_file_filter_lists"],
                self.paths["output_file_total"],
                self.paths["output_file_filter_total"],
            )
            user_getter.get_users()

    def list_expansion_all_account_types(self):
        """
        Performs the list expansion process
        for all the available account types
        """

        account_types = self.QUERIES
        for account_type in account_types:
            print(
                f" =============== PROCESSING: {account_type} ======================"
            )

            # Set account types and paths accordingly
            self.account_type = account_type
            self.setup_file_paths()
            self.perform_list_expansion()

    def list_expansion_all_locations(self):
        """
        Performs the list expansion process
        for all the available account types
        """

        for city in self.CITIES:
            print(f" =============== CITY: {city} ======================")
            self.location = city
            print(self.paths)
            self.list_expansion_all_account_types()

    def manual_list_expansion(self):
        """
        This function performs the list expansion for a
        manually added file
        """

        user_getter = UserGetter(
            self.location,
            self.paths["output_file_filter_lists"],
            self.paths["output_file_total"],
            self.paths["output_file_filter_total"],
        )
        user_getter.get_users()

    def filter_twitter_lists(self):
        """
        Filter the lists based on some pre-defined keywords.
        """
        list_reader = ListReader(self.paths["input_file_filter_lists"])
        self.lists_df = list_reader.create_df()
        if not self.lists_df.empty:
            list_filter = ListFilter(
                self.lists_df, self.paths["output_file_filter_lists"]
            )
            list_filter.keep_relevant_lists()
        else:
            print("No lists found")

    def reclassify_users(self):
        """
        Reclassify expanded twitter users.
        """
        print("Reclassifying expanded users based on location...")
        self.setup_file_paths()
        print(self.paths)
        self.user_filter = UserFilter(
            self.location,
            self.paths["output_file_total"],
            self.paths["output_file_filter_total"],
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
        Performs the re-classification process fir all accounts
        """
        account_types = list(self.QUERIES.keys())
        # Added manually added account types
        account_types.append('manually_added')
        for account_type in account_types:
            print(
                f" =============== PROCESSING: {account_type} ======================"
            )
            self.account_type = account_type
            self.reclassify_users()
