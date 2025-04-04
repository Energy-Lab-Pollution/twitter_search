"""
Script that handles the 'user_network.py' script to
generate a network from a particular city
"""
import json
import time
from pathlib import Path

import pandas as pd
from config_utils.constants import FIFTEEN_MINUTES
from network.user_network import UserNetwork


class NetworkHandler:
    """
    Class that handles the Twikit search and data collection process
    """

    FIFTEEN_MINUTES = FIFTEEN_MINUTES

    def __init__(self, location, num_users):
        self.location = location.lower()
        self.num_users = num_users

        self.base_dir = Path(__file__).parent.parent / "data/"
        # Users .csv with location matching
        self.users_file_path = (
            self.base_dir / "analysis_outputs/location_matches.csv"
        )
        # Building location output path
        self.location_file_path = (
            self.base_dir / f"networks/{self.location}.json"
        )

        # Get city users
        self.get_city_users()

    def get_city_users(self):
        """
        Method to get users whose location match the desired
        location
        """
        self.user_df = pd.read_csv(self.users_file_path)
        self.user_df = self.user_df.loc[
            self.user_df.loc[:, "search_location"] == self.location
        ]
        self.user_df = self.user_df.loc[
            self.user_df.loc[:, "location_match"], :
        ]
        self.user_df.reset_index(drop=True, inplace=True)

    def get_already_processed_users(self):
        """
        Reads the location JSON file and gets the
        set of users that have already been processed
        """
        users_list = []
        try:
            with open(self.location_file_path, "r") as f:
                existing_data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            existing_data = []
        if existing_data:
            for user_dict in existing_data:
                user_id = user_dict["user_id"]
                users_list.append(user_id)
        return users_list

    async def run(self):
        """
        Gets the user network data for a given number of
        users.

        Args:
            - num_users: Number of users to get data from
        """
        already_processed_users = self.get_already_processed_users()
        user_ids = self.user_df.loc[:, "user_id"].unique().tolist()

        for user_id in user_ids[: self.num_users]:
            if user_id not in already_processed_users:
                try:
                    user_network = UserNetwork(self.location_file_path)
                    print(f"Processing user {user_id}...")
                    await user_network.run(user_id)
                    time.sleep(self.FIFTEEN_MINUTES)
                except Exception as error:
                    print(f"Error getting user: {error}")
                    continue
            else:
                continue
