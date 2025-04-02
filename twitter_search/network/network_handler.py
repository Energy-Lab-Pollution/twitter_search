"""
Script that handles the 'user_network.py' script to
generate a network from a particular city
"""
import json
import os
import re
import time
from pathlib import Path

import pandas as pd
from config_utils.cities import ALIAS_DICT
from config_utils.constants import FIFTEEN_MINUTES
from network.user_network import UserNetwork


class NetworkHandler:
    """
    Class that handles the Twikit search and data collection process
    """

    FIFTEEN_MINUTES = FIFTEEN_MINUTES
    ALIAS_DICT = ALIAS_DICT

    def __init__(self, location, num_users):
        self.location = location.lower()
        self.num_users = num_users

        self.base_dir = Path(__file__).parent.parent / "data/"
        # Users .csv with location matching
        self.users_file_path = self.base_dir / "analysis_outputs/location_matches.csv"

        # Check if network path
        if not os.path.exists(self.base_dir / f"networks/{self.location}"):
            os.makedirs(self.base_dir / f"networks/{self.location}")
        else:
            print("Directory already exists")

        # Building location output path
        self.location_file_path = (
            self.base_dir / f"networks/{self.location}/{self.location}.json"
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
        self.user_df = self.user_df.loc[self.user_df.loc[:, "location_match"], :]
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

    @staticmethod
    def parse_edge_dict(source, target, tweet=None):
        """
        Parses soruce and target dictionaries to generate
        a single edge dictionary

        Args:
            - source (dict): Dict with source user info
            - target (dict): Dict with target user info
            - tweet (dict): Dict with original tweet info (Retweeters only)
        """
        edge_dict = {}
        edge_dict["source"] = source["user_id"]
        edge_dict["source_username"] = source["username"]
        edge_dict["source_followers"] = source["followers_count"]

        if tweet:
            edge_dict["tweet_id"] = tweet["tweet_id"]

        edge_dict["target"] = target["user_id"]
        edge_dict["target_username"] = target["username"]
        edge_dict["target_followers"] = target["followers_count"]

        return edge_dict

    def create_edges(self, edge_type):
        """
        Gets the existing JSON file and creates a list of dicts
        of the following form for retweeters or followers:
        {
            "edges": [
                {
                    "source": "user_id_1",
                    "target": "user_id_2",
                    "tweet_id": "tweet_id_1",
                    "target_username": "username2",
                    "target_followers": 200
                },
                ...
            ]
        }

        Note:
        'target' is the original user (which is followed / retweeted by someone)
        'source' is the user who follows or retweets the target user
        """
        edges = []
        graph_dict = {}
        graph_filename = (
            self.base_dir / f"networks/{self.location}/{edge_type}_interactions.json"
        )

        try:
            with open(self.location_file_path, "r") as f:
                existing_data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            existing_data = []

        if not existing_data:
            return

        if edge_type == "retweeter":
            for user_dict in existing_data:
                tweets = user_dict["tweets"]
                existing_tweets = []
                for tweet in tweets:
                    if "retweeters" in tweet and tweet["retweeters"]:
                        for retweeter in tweet["retweeters"]:
                            if tweet["tweet_id"] not in existing_tweets:
                                location_matches = self.check_location(
                                    retweeter["location"], self.location
                                )
                                if location_matches:
                                    retweeter_dict = self.parse_edge_dict(
                                        retweeter, user_dict, tweet
                                    )
                                    edges.append(retweeter_dict)

                                    # Add to arrays to track existing stuff
                                    existing_tweets.append(tweet["tweet_id"])
                            else:
                                continue

        else:
            for user_dict in existing_data:
                followers = user_dict["followers"]
                existing_followers = []
                for follower in followers:
                    if follower["user_id"] not in existing_followers:
                        location_matches = self.check_location(
                            follower["location"], self.location
                        )
                        if location_matches:
                            follower_dict = self.parse_edge_dict(
                                follower, user_dict, tweet=None
                            )
                            edges.append(follower_dict)
                            existing_followers.append(follower["user_id"])
                        else:
                            continue
                    else:
                        continue

        # Save JSON
        graph_dict["edges"] = edges
        with open(graph_filename, "w", encoding="utf-8") as file:
            json.dump(graph_dict, file, ensure_ascii=False, indent=4)
        print(f"Successfully stored {self.location} {edge_type} edges json file")

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
