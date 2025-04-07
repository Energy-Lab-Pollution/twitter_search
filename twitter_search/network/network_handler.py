"""
Script that handles the 'user_network.py' script to
generate a network from a particular city
"""
import json
import os
import re
import statistics
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
        self.users_file_path = (
            self.base_dir / "analysis_outputs/location_matches.csv"
        )

        # Check if network path
        if not os.path.exists(self.base_dir / f"networks/{self.location}"):
            os.makedirs(self.base_dir / f"networks/{self.location}")
        else:
            print("Directory already exists")

        # Building location output path
        self.location_file_path = (
            self.base_dir / f"networks/{self.location}/{self.location}.json"
        )

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

    @staticmethod
    def read_json(path):
        """
        Reads a JSON file and returns the data
        """
        try:
            with open(path, "r") as f:
                existing_data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            existing_data = []

        return existing_data

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

    def calculate_stats(self):
        """
        Calculates basic stats for retweeters / followers

        --- Number of root users

        --- Avg. Number of Retweeters per user
        - Number of retweeters (total agg)
        - Number of retweeters (twikit)
        - Number of retweeters (from the desired location)

        --- Avg. Number of Followers per user
        - Number of followers (total agg)
        - Number of followers (twikit)
        - Number of followers (from the desired

        """
        retweets_list = []
        followers_list = []
        twikit_retweeters = []
        twikit_followers = []
        city_retweeters = []
        city_followers = []
        original_tweets = []

        # Proportions
        perc_tweets_with_retweeters = []
        perc_tweets_with_retweeters_twikit = []

        # Paths setup
        follower_graph = self.read_json(
            self.base_dir
            / f"networks/{self.location}/follower_interactions.json"
        )
        retweeter_graph = self.read_json(
            self.base_dir
            / f"networks/{self.location}/retweet_interactions.json"
        )

        location_json = self.read_json(self.location_file_path)

        num_users = len(location_json)
        print(f"Number of root users {num_users}")

        for user_dict in location_json:
            user_city_followers = 0
            user_city_retweeters = 0
            num_original_tweets = 0
            num_tweets_with_retweeters = 0
            num_tweets_with_retweeters_twikit = 0
            if user_dict["followers_count"] > 0:
                followers_list.append(user_dict["followers_count"])
            twikit_followers.append(len(user_dict["followers"]))

            user_tweets = user_dict["tweets"]
            for user_tweet in user_tweets:
                # Remove reposts by others
                if (not user_tweet["tweet_text"].startswith("RT @")):
                    num_original_tweets += 1
                    if user_tweet["retweet_count"] > 0:
                        num_tweets_with_retweeters += 1
                        retweets_list.append(user_tweet["retweet_count"])
                    if "retweeters" in user_tweet:
                        if user_tweet["retweeters"]:
                            num_tweets_with_retweeters_twikit += 1
                            twikit_retweeters.append(len(user_tweet["retweeters"]))

            # Populate proportions array
            if num_original_tweets == 0:
                perc_tweets_with_retweeters.append(0)
            else:
                perc_tweets_with_retweeters.append(
                    num_tweets_with_retweeters / num_original_tweets
                )
            # Ignore cases when no tweets with retweeters are found
            if num_tweets_with_retweeters == 0:
                continue
            else:
                perc_tweets_with_retweeters_twikit.append(
                    num_tweets_with_retweeters_twikit
                    / num_tweets_with_retweeters
                )

            for follower_dict in follower_graph["edges"]:
                if follower_dict["target"] == user_dict["user_id"]:
                    user_city_followers += 1

            for retweeter_dict in retweeter_graph["edges"]:
                if retweeter_dict["target"] == user_dict["user_id"]:
                    user_city_retweeters += 1

            city_retweeters.append(user_city_retweeters)
            city_followers.append(user_city_followers)
            original_tweets.append(num_original_tweets)


        
        print(f"Median number of original tweets per user: {statistics.median(original_tweets)}")
        print("================= FOLLOWER STATS =====================")
        print(
            f"a) Median count of followers {statistics.median(followers_list)}"
        )
        print(
            f"b) Median twikit followers {statistics.median(twikit_followers)}"
        )
        print(
            (
                "c) Median twikit followers / median followers: "
                f"{statistics.median(twikit_followers) / statistics.median(followers_list)}"
            )
        )
        print(
            f"d) Median twikit followers in {self.location}: {statistics.median(city_followers)}"
        )
        print(
            (
                "e) Median twikit followers in kolkata / median twikit followers: "
                f"{statistics.median(twikit_followers) / statistics.median(followers_list)}"
            )
        )

        print("================= RETWEET STATS =====================")
        print(
            (
                "a) Median proportion of tweets per user that have at "
                f"least one all-time retweeter {statistics.median(perc_tweets_with_retweeters)}"
            )
        )
        print(
            f"b) Median sum of all-time retweeters per user: {statistics.median(retweets_list)}"
        )
        print(
            (
                "c) Median of num tweets with retweets / num tweets with twikit retweets: "
                f"{statistics.median(perc_tweets_with_retweeters_twikit)}"
            )
        )
        print(
            f"d) Median sum of twikit retweeters per user: {statistics.median(twikit_retweeters)}"
        )
        print(
            f"e) Median sum of twikit retweeters / median sum of all time retweeters: "
            f"{statistics.median(twikit_retweeters) / statistics.median(retweets_list)}"
        )
        print(
            f"f) Median retweeters with twikit in {self.location}: {statistics.median(city_retweeters)}"
        )
        print(
            f"g) Median sum of kolkata retweeters / median sum of twikit retweeters: "
            f"{statistics.median(city_retweeters) / statistics.median(twikit_retweeters)}"
        )

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
            self.base_dir
            / f"networks/{self.location}/{edge_type}_interactions.json"
        )

        try:
            with open(self.location_file_path, "r") as f:
                existing_data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            existing_data = []

        if not existing_data:
            return

        if edge_type == "retweet":
            for user_dict in existing_data:
                tweets = user_dict["tweets"]
                existing_tweets = []
                for tweet in tweets:
                    # If they retweeted someone else, skip
                    if tweet["tweet_text"].startswith("RT @"):
                        continue
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
        print(
            f"Successfully stored {self.location} {edge_type} edges json file"
        )

    async def run(self):
        """
        Gets the user network data for a given number of
        users.

        Args:
            - num_users: Number of users to get data from
        """
        # Get city users and already processed users
        self.get_city_users()
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
                print(f"Skipping {user_id} - they have already been processed")
                continue
