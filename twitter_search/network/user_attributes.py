"""
Script to add missing user attributes for Kolkata and Kanpur
"""

import json
import time
from datetime import datetime
from pathlib import Path

import twikit

# Local constants
from config_utils.cities import ALIAS_DICT
from config_utils.constants import (
    FIFTEEN_MINUTES,
    TWIKIT_COOKIES_DIR,
)
from config_utils.util import network_json_maker

class UserAttributes:
    """
    Class that handles the Twikit search and data collection process
    """

    FIFTEEN_MINUTES = FIFTEEN_MINUTES
    ALIAS_DICT = ALIAS_DICT

    def __init__(self, location):
        self.location = location.lower()
        self.base_dir = Path(__file__).parent.parent / "data/"
        # Building location output path
        self.location_file_path = (
            self.base_dir / f"networks/{self.location}/{self.location}.json"
        )
        self.retweet_edges_path = (
            self.base_dir / f"networks/{self.location}/retweet_interactions.json"
        )
        self.follower_edges_path = (
            self.base_dir / f"networks/{self.location}/follower_interactions.json"
        )
        self.new_location_file_path = (
            self.base_dir / f"networks/{self.location}/{self.location}_new.json"
        )
        # Global requests counter
        self.num_twikit_requests = 0

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
                success = True
            except twikit.errors.TooManyRequests:
                print("User Attributes: Too Many Requests")
                time.sleep(FIFTEEN_MINUTES)
            except twikit.errors.BadRequest:
                print("User Attributes: Bad Request")
                success = True
            except twikit.error.NotFound:
                print("User Attributes: Not Found")
                success = True

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

    async def run(self):
        """
        Gets the user attributes for root users, followers
        and retweeters
        """
        new_location_json = []

        # Get city users from the RAW JSON
        try:
            with open(self.location_file_path, "r") as f:
                existing_users = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            existing_users = []

        client = twikit.Client("en-US")
        client.load_cookies(TWIKIT_COOKIES_DIR)

        for user_dict in existing_users:
            tweets = user_dict["tweets"]
            followers = user_dict["followers"]
            print(f"Processing user {user_dict["user_id"]}...")
            # Getting
            user_attributes_dict= await self.get_user_attributes(
                    client, user_dict["user_id"]
                )
         
            # Adding attributes to retweeters
            new_user_tweets = []
            for tweet in tweets:
                if tweet["tweet_text"].startswith("RT @"):
                    continue
                if "retweeters" in tweet and tweet["retweeters"]:
                    new_tweet_dict = tweet.copy()
                    processed_retweeters = []
                    for retweeter in tweet["retweeters"]:
                        retweeter_attributes_dict = await self.get_user_attributes(
                            client, retweeter["user_id"]
                        )
                        processed_retweeters.append(retweeter_attributes_dict)                    
                    
                    new_tweet_dict["retweeters"] = processed_retweeters
                    new_user_tweets.append(new_tweet_dict)

            # Add newly processed tweets and retweeters
            user_attributes_dict["tweets"] = new_user_tweets
            
            # Procesing
            new_user_followers = []
            for follower in followers:
                # Only get attributes if file flag is true
                follower_attributes_dict= await self.get_user_attributes(
                        client, follower["user_id"]
                    )
                new_user_followers.append(follower_attributes_dict)
            # Add newly processed followers
            user_attributes_dict["followers"] = new_user_followers
            new_location_json.append(user_attributes_dict)
                        
        # New JSON saved with a new filename
        network_json_maker(self.new_location_file_path, new_location_json)
        print(f"Stored {user_dict["user_id"]} data")