"""
Script to search tweets and users from a particular location.
The users who match a certain criteria will be sent to a processing queue.
"""

import asyncio
import datetime
import json
import time
from argparse import ArgumentParser

import boto3
import twikit
import pandas as pd

from pathlib import Path
from config_utils.cities import CITIES_LANGS, LOCATION_ALIAS_DICT
from config_utils.constants import (
    EXPANSIONS,
    FIFTEEN_MINUTES,
    TWEET_FIELDS,
    TWIKIT_COOKIES_DICT,
    TWIKIT_TWEETS_THRESHOLD,
    USER_FIELDS,
)
from config_utils.queries import QUERIES_DICT
from config_utils.util import (
    check_location,
    client_creator,
    convert_to_iso_format,
)


class CityUsers:
    def __init__(self, location):
        self.base_dir = Path(__file__).parent/ "data/"
        self.location = location
        self.sqs_client = boto3.client("sqs", region_name="us-west-1")
        self.language = CITIES_LANGS[self.location]
        self.followers_threshold = 100

    def extract_queries_num_tweets(self, tweet_count):
        """
        This method gets all the queries with the appropiate aliases
        for the desired location, along with the allocated number of
        tweets for each query / account type

        Args
        ----------
            tweet_count (str): Total number of tweets to be distributed for each
                         account type

        """
        queries = QUERIES_DICT[self.language]

        if self.location in LOCATION_ALIAS_DICT:
            aliases = LOCATION_ALIAS_DICT[self.location]
        else:
            aliases = [self.location]

        num_tweets_per_account = tweet_count // len(queries)
        new_query_dict = {}

        for account_type in queries:
            print(
                f" =============== PROCESSING: {query} ======================"
            )
            query = queries[account_type]
            aliases_str = " OR ".join(aliases)
            # TODO: combination of all aliases
            query = query.replace("location", aliases_str)
            query = query.replace("\n", " ").strip()
            query = query.replace("  ", " ")
            query = query.replace("\t", " ")

            new_query_dict[account_type] = query

        return new_query_dict, num_tweets_per_account


    def parse_x_users(self, user_list):
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
        user_dicts = []
        for user in user_list:
            user_dict = {
                "user_id": user["id"],
                "username": user["username"],
                "description": user["description"],
                "profile_location": user["location"],
                "target_location": self.location,
                "verified": user["verified"],
                "created_at": user["created_at"],
                "processing_status": "pending",
            }
            for key, value in user["public_metrics"].items():
                user_dict[key] = value

            # TODO: Adding new attributes
            user_dict["category"] = "null"
            user_dict["treatment_arm"] = "null"
            user_dict["retweeter_status"] = "pending"
            user_dict["retweeter_last_processed"] = "null"
            user_dict["follower_status"] = "pending"
            user_dict["follower_last_processed"] = "null"
            user_dict["extracted_at"] = datetime.now().isoformat()
            user_dict["last_updated"] = datetime.now().isoformat()
            # See if location matches to add city
            location_match = check_location(user["location"], self.location)
            user_dict["city"] = self.location if location_match else None
            user_dicts.append(user_dict)

        return user_dicts

    def parse_twikit_users(self, users):
        """
        Parse retweeters (user objects) and put them
        into a list of dictionaries

        Args:
        ----------
            - tweets (list): list of twikit.User objects
        Returns:
        ----------
            - dict_list (list): list of dictionaries with users' info
        """
        users_list = []

        if users:
            for user in users:
                user_dict = {}
                user_dict["user_id"] = user.id
                user_dict["username"] = user.screen_name
                user_dict["description"] = user.description
                user_dict["profile_location"] = user.location
                user_dict["target_location"] = self.location
                user_dict["followers_count"] = user.followers_count
                user_dict["following_count"] = user.following_count
                user_dict["tweets_count"] = user.statuses_count
                user_dict["verified"] = user.verified
                user_dict["created_at"] = convert_to_iso_format(user.created_at)
                user_dict["category"] = "null"
                user_dict["treatment_arm"] = "null"
                user_dict["retweeter_status"] = "pending"
                user_dict["retweeter_last_processed"] = "null"
                user_dict["follower_status"] = "pending"
                user_dict["follower_last_processed"] = "null"
                user_dict["extracted_at"] = datetime.now().isoformat()
                user_dict["last_updated"] = datetime.now().isoformat()
                # See if location matches to add city
                location_match = check_location(user.location, self.location)
                user_dict["city"] = self.location if location_match else None
                users_list.append(user_dict)

        return users_list

    async def get_user_attributes(self, users_list, account_num):
        """
        Function to get all user attributes when we only get their
        ids from the existing file

        Args:
            - client: twikit.client object
            - user_id: str
        """
        client = twikit.Client("en-US")
        client.load_cookies(TWIKIT_COOKIES_DICT[f"account_num_{account_num}"])
        
        new_users_list = []
        for user in users_list:
            user_dict = {}
            user_dict["user_id"] = user['user_id']
            success = False
            while not success:
                # Get source user information
                try:
                    user_obj = await client.get_user_by_id(user['user_id'])
                except twikit.errors.TooManyRequests:
                    print("User Attributes: Too Many Requests...")
                    time.sleep(FIFTEEN_MINUTES)
                    user_obj = await client.get_user_by_id(user['user_id'])
                except twikit.errors.BadRequest:
                    print("User Attributes: Bad Request")
                    continue
                except twikit.errors.NotFound:
                    print("User Attributes: Not Found")
                    continue
                except twikit.errors.TwitterException as err:
                    print(f"User Attributes: Twitter Error ({err} - {user['user_id']})")
                    continue
                user_dict["user_id"] = user_obj.id
                user_dict["username"] = user_obj.screen_name
                user_dict["description"] = user_obj.description
                user_dict["profile_location"] = user_obj.location
                user_dict["target_location"] = self.location
                user_dict["followers_count"] = user_obj.followers_count
                user_dict["following_count"] = user_obj.following_count
                user_dict["tweets_count"] = user_obj.statuses_count
                user_dict["verified"] = user_obj.verified
                user_dict["created_at"] = convert_to_iso_format(user_obj.created_at)
                user_dict["category"] = "null"
                user_dict["treatment_arm"] = "null"
                user_dict["retweeter_status"] = "pending"
                user_dict["retweeter_last_processed"] = "null"
                user_dict["follower_status"] = "pending"
                user_dict["follower_last_processed"] = "null"
                user_dict["extracted_at"] = datetime.now().isoformat()
                user_dict["last_updated"] = datetime.now().isoformat()

                # See if location matches to add city
                location_match = check_location(
                    user_obj.location, self.location
                )
                user_dict["city"] = self.location if location_match else None
                new_users_list.append(user_dict)
                success = True

        return new_users_list

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
        users_list = self.user_df.loc[:, "user_id"].astype(str).unique().tolist()

        return users_list

    async def _get_twikit_city_users(self, queries_dict, num_tweets, account_num):
        """
        Method used to search for tweets, with twikit,
        using a given query

        This method uses twikit's "await next" function
        to get more tweets with the given query. The corresponding
        users are then parsed from such tweets.

        Args:
            - queries_dict (dict): Query dictionary with keywords and location
            - num_tweets (int): Determines the number of tweets to use **per query**
        """
        client = twikit.Client("en-US")
        client.load_cookies(TWIKIT_COOKIES_DICT[f"account_num_{account_num}"])
        users_list = []
        num_iter = 0
        more_tweets_available = True

        for query in queries_dict:

            try:
                tweets = await client.search_tweet(
                    query, "Latest", count=num_tweets
                )
            except twikit.errors.TooManyRequests:
                time.sleep(FIFTEEN_MINUTES)
                tweets = await client.search_tweet(
                    query, "Latest", count=num_tweets
                )          

            # TODO: Add set operation to keep unique users only
            users_list = self.parse_twikit_users(tweets)
            while more_tweets_available:
                num_iter += 1
                try:
                    if num_iter == 1:
                        next_tweets = await tweets.next()
                    else:
                        next_tweets = await next_tweets.next()
                    if next_tweets:
                        # Parse next tweets
                        next_users_list = self.parse_twikit_users(next_tweets)
                        users_list.extend(next_users_list)
                    else:
                        more_tweets_available = False 
                except twikit.errors.TooManyRequests:
                        print("Tweets: too many requests, sleeping...")
                        time.sleep(FIFTEEN_MINUTES)
                if num_iter % 5 == 0:
                    print(f"Processed {num_iter} batches")
                # Leave process running until tweets are recollected

        return users_list

    def _get_x_city_users(self, queries_dict, num_tweets):
        """
        Method used to search for tweets, with the X API,
        using a given query

        The corresponding users are then parsed from
        such tweets.
        """
        x_client = client_creator()
        result_count = 0
        next_token = None
        users_list = []
        tweets_list = []

        for query in queries_dict:

            # TODO: Check if this is correct
            while result_count < num_tweets:
                print(f"Max results is: {result_count}")
                response = x_client.search_recent_tweets(
                    query=query,
                    max_results=num_tweets,
                    next_token=next_token,
                    expansions=EXPANSIONS,
                    tweet_fields=TWEET_FIELDS,
                    user_fields=USER_FIELDS,
                )
                if response.meta["result_count"] == 0:
                    print("No more results found.")
                    break
                result_count += response.meta["result_count"]
                tweets_list.extend(response.data)
                users_list.extend(response.includes["users"])
                users_list = self.parse_x_users(users_list)
                try:
                    next_token = response.meta["next_token"]
                except Exception as err:
                    print(err)
                    next_token = None

                if next_token is None:
                    break

        return users_list

    def send_to_queue(self, users_list, queue_name):
        """
        Sends twikit or X users to the corresponding queue

        Args:
            - users_list (list)
            - queue_name (str)
        """
        user_tweets_queue_url = self.sqs_client.get_queue_url(
            QueueName=queue_name
        )["QueueUrl"]
        for user in users_list:
            if user["city"] and user["num_followers"] > self.followers_threshold:
                print(f"Sending user {user['user_id']}")
                message = {
                    "user_id": user['user_id'],
                    "location": self.location,
                }
                try:
                    # Send to user tweets
                    self.sqs_client.send_message(
                            QueueUrl=user_tweets_queue_url,
                            MessageBody=json.dumps(message),
                        )
                    print("Success :)")
                except Exception as err:
                    print(f"Unable to send user {user['user_id']} to  {queue_name} SQS: {err}")
                break


if __name__ == "__main__":
    # parameters: [location, tweet_count, keywords (both hashtags, timeperiod and keywords)]
    parser = ArgumentParser(
        "Parameters to get users data to generate a network"
    )
    parser.add_argument(
        "--location", type=str, help="Location to search users from"
    )
    parser.add_argument(
        "--tweet_count", type=str, help="Number of tweets to get"
    )
    parser.add_argument(
        "--extraction_type",
        type=str,
        choices=["file", "twikit", "X"],
        help="Choose how to get users",
    )
    parser.add_argument(
        "--account_num",
        type=int,
        help="Account number to use with twikit",
    )
    args = parser.parse_args()

    city_users = CityUsers(args.location)

    if args.extraction_type == "twikit":
        new_query_dict, num_tweets_per_account = city_users.extract_queries_num_tweets(args.tweet_count)
        users_list = asyncio.run(city_users._get_twikit_city_users(num_tweets_per_account, new_query_dict, args.account_num))
    elif args.extraction_type == "X":
        new_query_dict, num_tweets_per_account = city_users.extract_queries_num_tweets(args.tweet_count)
        users_list = city_users._get_x_city_users(num_tweets_per_account, new_query_dict)
    elif args.extraction_type == "file":
        users_list = city_users._get_file_city_users()
        print("Got users from file")
        users_list = city_users.get_user_attributes(users_list, args.account_num)
        print("Getting user attributes")

    city_users.send_to_queue(users_list, "UserTweets")
    # city_users.send_to_queue(users_list, "UserFollowers")

    # TODO: Insert / Check city node
    # TODO: Upload user attributes to Neptune -- Neptune handler class
