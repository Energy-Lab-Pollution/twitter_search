"""
Script to add missing user attributes for Kolkata and Kanpur
"""

import json
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import twikit

# Local constants
from config_utils.cities import ALIAS_DICT
from config_utils.constants import (
    FIFTEEN_MINUTES,
    MAX_RESULTS,
    TWIKIT_COOKIES_DIR,
    TWIKIT_COUNT,
    TWIKIT_TWEETS_THRESHOLD,
)
from config_utils.util import client_creator, load_json
from network.user_network import UserNetwork

class UserAttributes:
    """
    Class that handles the Twikit search and data collection process
    """

    FIFTEEN_MINUTES = FIFTEEN_MINUTES
    ALIAS_DICT = ALIAS_DICT

    def __init__(self, location):
        self.location = location.lower()
        self.base_dir = Path(__file__).parent.parent / "data/"
        # Check if network path exists
        if not os.path.exists(self.base_dir / f"networks/{self.location}"):
            os.makedirs(self.base_dir / f"networks/{self.location}")
        else:
            print("Directory already exists")

        # Building location output path
        self.location_file_path = (
            self.base_dir / f"networks/{self.location}/{self.location}.json"
        )

    async def get_root_user_attributes(self, client, user_id):
        """
        Function to get all user attributes when we only get their
        ids from the existing file

        Args:
            - client: twikit.client object
            - user_id: str
        """
        user_dict = {}
        user_dict["user_id"] = user_id

        # Get source user information
        user_obj = await client.get_user_by_id(user_id)
        user_dict["username"] = user_obj.screen_name
        user_dict["profile_location"] = user_obj.location
        user_dict["followers_count"] = user_obj.followers_count
        user_dict["following_count"] = user_obj.following_count
        user_dict["tweets_count"] = user_obj.statuses_count
        user_dict["verified"] = user_obj.verified
        user_dict["created_at"] = user_obj.created_at
        user_dict["target_location"] = self.location
        user_dict["city"]

        # TODO: Adding new attributes
        user_dict["category"] = None
        user_dict["treatment_arm"] = None
        user_dict["processing_status"] = "pending"
        user_dict["extracted_at"] = datetime.now()
        user_dict["last_updated"] = datetime.now()
        user_dict["last_processed"] = None

        return user_dict

    async def create_user_network(self, extraction_type, file_flag):
        """
        Gets the user network data for a given number of
        users.

        Args:
            - extraction_type (str): Determines if the users' network data will be
            obtained via twikit or X
            - file_flag (boolean): Determines
        """
        # Get city users and users to process
        users_list = self._get_city_users(extraction_type)
        # list of user dicts that gets proccessed (no ids )
        client = twikit.Client("en-US")
        client.load_cookies(TWIKIT_COOKIES_DIR)

        for user_to_process in users_list:
            try:
                # Only get attributes if file flag is true
                user_to_process_dict = await self.get_root_user_attributes(
                        client, user_to_process
                    )
                print(f"Processing user {user_to_process}...")
                # TODO: user_id will come from a queue
            except Exception as error:
                print(f"Error getting user: {error}")
                continue
