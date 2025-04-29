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
import twikit

# Local constants
from config_utils.cities import ALIAS_DICT
from config_utils.constants import (
    FIFTEEN_MINUTES,
    TWIKIT_COOKIES_DIR,
    TWIKIT_COUNT,
    TWIKIT_TWEETS_THRESHOLD,
)
from config_utils.util import client_creator, load_json
from config_utils.constants import MAX_RESULTS, EXPANSIONS, TWEET_FIELDS, USER_FIELDS
from network.user_network import UserNetwork


class NetworkHandler:
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

    def parse_twikit_users(self, tweets):
        """
        Parses users from the found tweets into dictionaries

        Args:
            tweets: Array of twikit.tweet.Tweet objects

        Returns:
            tweets_list: Array of dictionaries with tweets' data
            users_list: Array of dictionaries with users' data
        """
        users_list = []
        for tweet in tweets:
            user_dict = {}
            user_dict["user_id"] = tweet.user.id
            user_dict["username"] = tweet.user.name
            user_dict["description"] = tweet.user.description
            user_dict["profile_location"] = tweet.user.location
            user_dict["target_location"] = self.location
            user_dict["followers_count"] = tweet.user.followers_count
            user_dict["following_count"] = tweet.user.following_count
            user_dict["tweets_count"] = tweet.user.statuses_count
            # TODO: Check difference between verified and is_blue_verified
            user_dict["verified"] = tweet.user.verified
            # TODO: Ask if we will need the ones below..
            user_dict["tweet_id"] = tweet.id
            user_dict["tweets"] = [tweet.text]



            users_list.append(user_dict)

        return users_list

    def parse_x_users(user_list):
        """
        This function takes a list of user objects and
        transformis it into a list of dictionaries

        Parameters
        ----------
        user_list : list
            List of user objects

        Returns
        -------
        dict_list: list
            List of dictionaries with user data
        """
        dict_list = []
        for user in user_list:
            values = {
                "user_id": user["id"],
                "username": user["username"],
                "description": user["description"],
                "profile_location": user["location"],
                "name": user["name"],
                "url": user["url"],
                "tweets": [],
                "geo_code": [],
            }
            values.update(user["public_metrics"])
            dict_list.append(values)
        return dict_list


    async def _get_twikit_city_users(self):
        """
        Method used to search for tweets, with twikit,
        using a given query

        This method uses twikit's "await next" function
        to get more tweets with the given query. The corresponding
        users are then parsed from such tweets.
        """
        client = twikit.Client("en-US")
        client.load_cookies(TWIKIT_COOKIES_DIR)

        tweets = await client.search_tweet(
            self.location, "Latest", count=TWIKIT_COUNT
        )
        self.users_list = self.parse_twikit_users(tweets)

        more_tweets_available = True
        num_iter = 1

        next_tweets = await tweets.next()
        if next_tweets:
            next_users_list = self.parse_twikit_users(next_tweets)
            self.users_list.extend(next_users_list)
        else:
            more_tweets_available = False

        while more_tweets_available:
            next_tweets = await next_tweets.next()
            if next_tweets:
                next_users_list = self.parse_twikit_users(next_tweets)
                self.users_list.extend(next_users_list)

            else:
                more_tweets_available = False

            if num_iter % 5 == 0:
                print(f"Processed {num_iter} batches")

            # TODO: We may leave this entire process running
            if num_iter == TWIKIT_TWEETS_THRESHOLD:
                break

            num_iter += 1

    def _get_x_city_users(self):
        """
        Method used to search for tweets, with the X API,
        using a given query

        The corresponding users are then parsed from 
        such tweets.
        """
        x_client = client_creator()
        result_count = 0
        next_token = None

        # TODO: Only return / keep user ids
        while result_count < MAX_RESULTS:
            print(f"Max results is: {result_count}")
            response = x_client.search_recent_tweets(
                query=self.location,
                max_results=MAX_RESULTS,
                next_token=next_token,
                expansions=EXPANSIONS,
                tweet_fields=TWEET_FIELDS,
                user_fields=USER_FIELDS,
            )
            if response.meta["result_count"] == 0:
                print("No more results found.")
                break
            result_count += response.meta["result_count"]
            self.total_tweets.extend(response.data)
            self.total_users.extend(response.includes["users"])
            try:
                next_token = response.meta["next_token"]
            except Exception as err:
                print(err)
                next_token = None

            if next_token is None:
                break

    def _get_file_city_users(self):
        """
        Method to get users whose location match the desired
        location
        """
        # Users .csv with location matching
        users_file_path = (
            self.base_dir / "analysis_outputs/location_matches.csv"
        )
        self.user_df = pd.read_csv(users_file_path)
        self.user_df = self.user_df.loc[
            self.user_df.loc[:, "search_location"] == self.location
        ]
        self.user_df = self.user_df.loc[
            self.user_df.loc[:, "location_match"], :
        ]
        self.user_df.reset_index(drop=True, inplace=True)

        print(f"Users in .csv: {len(self.user_df)}")

    async def _get_city_users(self, extraction_type):
        """
        Searches for city users either from:
            - twikit
            - location_matches.csv file

        The method only returns users who have a location match
        and haven't been processed before
        """
        if extraction_type == "twikit":
            self.user_ids = []
            await self._get_twikit_city_users()
            for user in self.users_list:
                location_match = self.check_location(
                    user["location"], self.location
                )

                if location_match:
                    self.user_ids.append[user["user_id"]]
                    # TODO: Check if user has been processed before
                    # TODO: Upload user data to DynamoDB (followers, location, etc)
                    # TODO: Send user_id to SQS queue to get network data
                else:
                    # TODO: Handle users whose location doesn't match
                    pass

        elif extraction_type == "file":
            self._get_file_city_users()
            self.already_processed_users = self._get_already_processed_users()
            self.user_ids = self.user_df.loc[:, "user_id"].unique().tolist()

        # X official API
        else:
            self._get_x_city_users()

    def _get_already_processed_users(self):
        """
        Reads the location JSON file and gets the
        set of users that have already been processed
        """
        users_list = []
        existing_data = load_json(self.location_file_path)

        if existing_data:
            for user_dict in existing_data:
                user_id = user_dict["user_id"]
                users_list.append(user_id)
        return users_list

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

    def calculate_rt_queries(self, location_json):
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
        perc_retweets_queried = []
        perc_original_tweets_queried = []

        for user_dict in location_json:
            retweets_queried = 0
            original_tweets_queried = 0
            total = 0
            user_tweets = user_dict["tweets"]
            for user_tweet in user_tweets:
                # Remove reposts by others
                if (user_tweet["tweet_text"].startswith("RT @")) and (
                    "retweeters" in user_tweet
                ):
                    retweets_queried += 1
                else:
                    if "retweeters" in user_tweet:
                        original_tweets_queried += 1

            total += retweets_queried + original_tweets_queried
            if total == 0:
                continue
            perc_retweets_queried.append(retweets_queried / total)
            perc_original_tweets_queried.append(original_tweets_queried / total)

        print("================ RT QUERIES =================")
        print(
            f"a) Median RTs queried by Twikit {statistics.median(perc_retweets_queried)}"
        )
        print(
            f"b) Median original tweets queried by Twikit {statistics.median(perc_original_tweets_queried)}"
        )

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
        follower_graph = load_json(
            self.base_dir
            / f"networks/{self.location}/follower_interactions.json"
        )
        retweeter_graph = load_json(
            self.base_dir
            / f"networks/{self.location}/retweet_interactions.json"
        )

        location_json = load_json(self.location_file_path)

        num_users = len(location_json)
        print(f"Number of root users {num_users}")

        for user_dict in location_json:
            user_retweeters = 0
            user_twikit_retweeters = 0
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
                if not user_tweet["tweet_text"].startswith("RT @"):
                    num_original_tweets += 1
                    if user_tweet["retweet_count"] > 0:
                        num_tweets_with_retweeters += 1
                        user_retweeters += user_tweet["retweet_count"]

                    if "retweeters" in user_tweet:
                        if user_tweet["retweeters"]:
                            num_tweets_with_retweeters_twikit += 1
                            user_twikit_retweeters += len(
                                user_tweet["retweeters"]
                            )

            retweets_list.append(user_retweeters)
            twikit_retweeters.append(user_twikit_retweeters)
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

        print(
            f"Median number of original tweets per user: {round(statistics.median(original_tweets), 2)}"
        )
        print("================= FOLLOWER STATS =====================")
        print(
            f"a) Median count of followers {round(statistics.median(followers_list), 2)}"
        )
        print(
            f"b) Median twikit followers {round(statistics.median(twikit_followers), 2)}"
        )
        print(
            (
                "c) Median twikit followers / median followers: "
                f"{round(statistics.median(twikit_followers) / statistics.median(followers_list), 2)}"
            )
        )
        print(
            f"d) Median twikit followers in {self.location}: {round(statistics.median(city_followers), 2)}"
        )
        print(
            (
                "e) Median twikit followers in kolkata / median twikit followers: "
                f"{round(statistics.median(city_followers) / statistics.median(twikit_followers), 2)}"
            )
        )
        print("================= RETWEET STATS =====================")
        print(
            (
                "a) Median proportion of tweets per user that have at "
                f"least one all-time retweeter {round(statistics.median(perc_tweets_with_retweeters), 2)}"
            )
        )
        print(
            f"b) Median sum of all-time retweeters per user: {statistics.median(retweets_list)}"
        )
        print(
            (
                "c) Median of num tweets with retweets / num tweets with twikit retweets: "
                f"{round(statistics.median(perc_tweets_with_retweeters_twikit), 2)}"
            )
        )
        print(
            f"d) Median sum of twikit retweeters per user: {statistics.median(twikit_retweeters)}"
        )
        print(
            f"e) Median sum of twikit retweeters / median sum of all time retweeters: "
            f"{round(statistics.median(twikit_retweeters) / statistics.median(retweets_list), 2)}"
        )
        print(
            f"f) Median retweeters with twikit in {self.location}: {statistics.median(city_retweeters)}"
        )
        print(
            f"g) Median sum of kolkata retweeters / median sum of twikit retweeters: "
            f"{round(statistics.median(city_retweeters) / statistics.median(twikit_retweeters), 2)}"
        )
        self.calculate_rt_queries(location_json)

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

    async def create_user_network(self, extraction_type):
        """
        Gets the user network data for a given number of
        users.

        Args:
            - num_users: Number of users to get data from
        """
        # Get city users and users to process
        await self._get_city_users(extraction_type)
        users_to_process = list(
            set(self.user_ids).difference(set(self.already_processed_users))
        )

        # TODO: Add check location for users

        for user_to_process in users_to_process:
            try:
                user_network = UserNetwork(self.location_file_path, self.location)
                print(f"Processing user {user_to_process}...")
                # TODO: user_id will come from a queue
                await user_network.run(user_to_process)
                time.sleep(self.FIFTEEN_MINUTES)
            except Exception as error:
                print(f"Error getting user: {error}")
                continue
