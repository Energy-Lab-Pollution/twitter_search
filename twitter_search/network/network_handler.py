"""
Script that handles the 'user_network.py' script to
generate a network from a particular city
"""
import time
from pathlib import Path

import pandas as pd
from config_utils.constants import FIFTEEN_MINUTES
from network.user_network import UserNetwork


class NetworkHandler:
    """
    Class that handles the Twikit search and data collection process
    """

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

        # Instantiate user network class
        self.user_network = UserNetwork(self.location_file_path)
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
        self.user_df.reset_index(drop=True, inplace=True)

    async def run(self):
        """
        Gets the user network data for a given number of
        users.

        Args:
            - num_users: Number of users to get data from
        """
        user_ids = self.user_df.loc[:, "user_id"].unique().tolist()
        for user_id in user_ids[: self.num_users]:
            print(f"Processing user {user_id}...")
            await self.user_network.run(user_id)
            time.sleep(FIFTEEN_MINUTES)
