"""
Script to add missing user attributes for Kolkata and Kanpur
"""

import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import twikit

# Local constants
from config_utils.cities import ALIAS_DICT
from config_utils.constants import (
    FIFTEEN_MINUTES,
    SIXTEEN_MINUTES,
)
from config_utils.util import (
    convert_to_iso_format,
    load_json,
    network_json_maker,
)
from tqdm import tqdm


class UserAttributes:
    """
    Class that handles the Twikit search and data collection process
    """

    FIFTEEN_MINUTES = FIFTEEN_MINUTES
    ALIAS_DICT = ALIAS_DICT
    PROBLEMATIC_USERS = ["1249023238488768512", "147195485"]

    def __init__(self, location):
        self.location = location.lower()
        self.base_dir = Path(__file__).parent.parent / "data/"
        # Building location output path
        self.location_file_path = (
            self.base_dir / f"networks/{self.location}/{self.location}.json"
        )
        self.new_location_file_path = (
            self.base_dir / f"networks/{self.location}/{self.location}_new.json"
        )
        self.location_users_path = (
            self.base_dir
            / f"networks/{self.location}/{self.location}_users.json"
        )

    @staticmethod
    def check_location(raw_location, target_location):
        """
        Uses regex to see if the raw location matches
        the target location
        """

        target_locations = [target_location]

        # alias is the key, target loc is the value
        for alias, value in ALIAS_DICT.items():
            if value == target_location:
                target_locations.append(alias)

        if isinstance(raw_location, str):
            raw_location = raw_location.lower().strip()
            location_regex = re.findall(r"\w+", raw_location)

            if location_regex:
                for target_location in target_locations:
                    if target_location in location_regex:
                        return True
                    elif target_location in raw_location:
                        return True
                else:
                    return False
            else:
                return False
        else:
            return False

    async def get_user_attributes(self, client, user_id):
        """
        Function to get all user attributes when we only get their
        ids from the existing file

        Args:
            - client: twikit.client object
            - user_id: str
        """
        user_dict = {}
        user_dict["user_id"] = user_id
        success = False

        while not success:
            # Get source user information
            try:
                user_obj = await client.get_user_by_id(user_id)
            except twikit.errors.TooManyRequests:
                print("User Attributes: Too Many Requests...")
                time.sleep(SIXTEEN_MINUTES)
                user_obj = await client.get_user_by_id(user_id)
            except twikit.errors.BadRequest:
                print("User Attributes: Bad Request")
                return user_dict
            except twikit.errors.NotFound:
                print("User Attributes: Not Found")
                return user_dict
            # except twikit.errors.TwitterException as err:
            #     print(f"User Attributes: Twitter Error ({err} - {user_id})")
            #     return user_dict
            user_dict["user_id"] = user_obj.id
            user_dict["username"] = user_obj.screen_name
            user_dict["description"] = user_obj.description
            user_dict["profile_location"] = user_obj.location
            user_dict["target_location"] = self.location
            user_dict["followers_count"] = user_obj.followers_count
            user_dict["following_count"] = user_obj.following_count
            user_dict["tweets_count"] = user_obj.statuses_count
            # TODO: Check difference between verified and is_blue_verified
            user_dict["verified"] = user_obj.verified
            user_dict["created_at"] = convert_to_iso_format(user_obj.created_at)
            # TODO: Adding new attributes
            user_dict["category"] = None
            user_dict["treatment_arm"] = None
            user_dict["processing_status"] = "pending"
            user_dict["extracted_at"] = datetime.now().isoformat()
            # TODO: Needs to be a value of our choice
            last_date = datetime.now() - timedelta(days=14)
            user_dict["last_processed"] = last_date.isoformat()
            user_dict["last_updated"] = datetime.now().isoformat()

            # See if location matches to add city
            location_match = self.check_location(
                user_obj.location, self.location
            )
            user_dict["city"] = self.location if location_match else None
            success = True

        return user_dict

    def store_user_attributes(self, user_attributes_dict):
        """
        Stores the user attributes on the user file path.

        Args:
        ------
        user_attributes_dict: Dictionary with user data
        """

        network_json_maker(self.location_users_path, [user_attributes_dict])

    def user_attributes_exist(self, user_id):
        """
        Determines if the user has already been processed or not
        """
        existing_users = load_json(self.location_users_path)
        user_ids = [str(user["user_id"]) for user in existing_users]

        if str(user_id) in user_ids:
            return True
        else:
            return False

    def load_user_attributes(self, user_id):
        """
        Loads the user attributes from the predetermined JSON
        """
        existing_users = load_json(self.location_users_path)
        for existing_user in existing_users:
            if user_id == existing_user["user_id"]:
                return existing_user

    def _get_already_processed_users(self):
        """
        Reads the location JSON file and gets the
        set of users that have already been processed
        """
        users_list = []
        existing_data = load_json(self.new_location_file_path)

        if existing_data:
            for user_dict in existing_data:
                user_id = user_dict["user_id"]
                users_list.append(user_id)
        return users_list

    async def run(self):
        """
        Gets the user attributes for root users, followers
        and retweeters
        """

        # Already re-processed users with all attributes:
        # processed_users = self._get_already_processed_users()

        # Get city users from the RAW JSON
        with open(self.new_location_file_path, "r") as f:
            users_list = json.load(f)

        for user_dict in users_list:
            tweets = user_dict["tweets"]
            followers = user_dict["followers"]

            print(f"Processing user {user_dict["user_id"]}...")
            if not self.user_attributes_exist(user_dict["user_id"]):
                user_dict_copy = user_dict.copy()
                del user_dict_copy["tweets"]
                del user_dict_copy["followers"]
                if "tweets" in user_dict_copy:
                    raise ValueError("Shouldnt have happened")
                self.store_user_attributes(user_dict_copy)

            # Adding attributes to retweeters
            retweeters = []
            print("Processing retweeters..")
            for tweet in tqdm(tweets):
                if tweet["tweet_text"].startswith("RT @"):
                    continue
                if "retweeters" in tweet and tweet["retweeters"]:
                    for retweeter in tqdm(tweet["retweeters"]):
                        retweeters.append(retweeters)
                    if not self.user_attributes_exist(retweeter["user_id"]):
                        self.store_user_attributes(retweeter)

            # Procesing
            print("Processing followers")
            for follower in tqdm(followers):
                if not self.user_attributes_exist(follower["user_id"]):
                    self.store_user_attributes(follower)
