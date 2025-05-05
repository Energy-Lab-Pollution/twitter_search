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
    TWIKIT_FDM_COOKIES_DIR,
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
        # Global requests counter
        self.num_twikit_requests = 0

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
        new_location_json = []

        # Already re-processed users with all attributes:
        processed_users = self._get_already_processed_users()

        # Get city users from the RAW JSON
        with open(self.location_file_path, "r") as f:
            users_list = json.load(f)

        client = twikit.Client("en-US")
        client.load_cookies(TWIKIT_FDM_COOKIES_DIR)

        for user_dict in users_list:
            tweets = user_dict["tweets"]
            followers = user_dict["followers"]
            if (str(user_dict["user_id"]) in processed_users) or (
                str(user_dict["user_id"]) in self.PROBLEMATIC_USERS
            ):
                print(f"Already processed {user_dict['user_id']}.. skipping")
                continue

            print(f"Processing user {user_dict["user_id"]}...")
            # Getting add
            user_attributes_dict = await self.get_user_attributes(
                client, str(user_dict["user_id"])
            )
            if "last_processed" not in user_attributes_dict:
                continue
            # Adding attributes to retweeters
            new_user_tweets = []
            print("Processing retweeters..")
            for tweet in tqdm(tweets):
                if tweet["tweet_text"].startswith("RT @"):
                    continue
                if "retweeters" in tweet and tweet["retweeters"]:
                    new_tweet_dict = tweet.copy()
                    processed_retweeters = []
                    for retweeter in tqdm(tweet["retweeters"]):
                        try:
                            retweeter_attributes_dict = (
                                await self.get_user_attributes(
                                    client, str(retweeter["user_id"])
                                )
                            )
                            processed_retweeters.append(
                                retweeter_attributes_dict
                            )
                        except Exception as error:
                            print(
                                f"Error processing {retweeter['user_id']} - {error}"
                            )
                            if "Twitter Error" not in str(error):
                                processed_retweeters.append(retweeter)
                            else:
                                continue
                    new_tweet_dict["retweeters"] = processed_retweeters
                    new_user_tweets.append(new_tweet_dict)

            # Add newly processed tweets and retweeters
            user_attributes_dict["tweets"] = new_user_tweets

            # Procesing
            print("Processing followers")
            new_user_followers = []
            for follower in tqdm(followers):
                # Only get attributes if file flag is true
                try:
                    follower_attributes_dict = await self.get_user_attributes(
                        client, str(follower["user_id"])
                    )
                    new_user_followers.append(follower_attributes_dict)
                except Exception as error:
                    print(f"Error processing {retweeter['user_id']} - {error}")
                    if "Twitter Error" not in str(error):
                        new_user_followers.append(follower)
                    else:
                        continue
            # Add newly processed followers
            user_attributes_dict["followers"] = new_user_followers
            new_location_json.append(user_attributes_dict)
            # New JSON saved with a new filename
            network_json_maker(
                self.new_location_file_path, [user_attributes_dict]
            )
            print(f"Stored {user_dict["user_id"]} data")
