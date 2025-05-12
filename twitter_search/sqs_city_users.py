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
from config_utils.cities import ALIAS_DICT, CITIES_LANGS
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
    def __init__(self, location, tweet_count, extraction_type, account_num=1):
        self.base_dir = Path(__file__).parent/ "data/"
        self.location = location
        self.sqs_client = boto3.client("sqs", region_name="us-west-1")
        self.tweet_count = tweet_count
        self.extraction_type = extraction_type
        self.account_num = account_num

        # Define main city depending on if we're using an alias
        if self.location in ALIAS_DICT:
            print(f"{self.location} found in alias dict")
            self.main_city = ALIAS_DICT[self.location]

            print(
                f"Getting language and queries for {self.location} - {self.main_city}"
            )
        else:
            self.main_city = location

        language = CITIES_LANGS[self.main_city]
        self.queries = QUERIES_DICT[language]

    def get_account_type_tweets(self, num_account_types):
        """
        Gets number of requests for each particular account type.

        Twikit can only process 50 requests to get tweets in a 15 min
        interval. Therefore, for several cities, we need to determine
        how many requests each city will get.

        Args:
            city_requests: int determining the number of requests per city
        """
        account_requests = self.tweet_count / num_account_types
        remainder_requests = self.tweet_count % num_account_types

        if account_requests < 1:
            print(
                "Account requests: Not enough requests to extract all accounts"
            )
            return None

        # Create list of num requests per account
        tweet_counts_list = []
        for _ in range(0, len(account_requests)):
            # round to nearest int
            account_requests = round(account_requests)
            tweet_counts_list.append(account_requests)

        # If remainder exists, add to last account
        if remainder_requests > 0:
            num_requests = tweet_counts_list[-1]
            num_requests += remainder_requests
            tweet_counts_list[-1] = num_requests

        return tweet_counts_list

    def run_all_account_types(self, skip_media=False):
        """
        Runs the entire process for all the available
        account types for a particular location.

        Args
        ----------
            skip_media: str
                Determines if we should skip the search for media accounts
                (there are tons of them)

        """
        account_types = self.queries.copy()
        self.skip_media = skip_media

        if skip_media:
            if "media" in account_types:
                del account_types["media"]
                # Number of account types
        num_account_types = len(account_types)
        accounts_num_tweets = self.get_account_type_tweets(num_account_types)

        for account_type, account_num_tweets in zip(
            account_types, accounts_num_tweets
        ):
            print(
                f" =============== PROCESSING: {query} ======================"
            )
            query = self.queries[account_type]
            query = query.replace("location", self.location)
            query = query.replace("\n", " ").strip()
            query = query.replace("  ", " ")
            query = query.replace("\t", " ")

            self._get_city_users(account_num_tweets, query)


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
            location_match = check_location(user["location"], self.main_city)
            user_dict["city"] = self.main_city if location_match else None
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
                location_match = check_location(user.location, self.main_city)
                user_dict["city"] = self.main_city if location_match else None
                users_list.append(user_dict)

        return users_list

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

    async def _get_twikit_city_users(self, num_tweets, query):
        """
        Method used to search for tweets, with twikit,
        using a given query

        This method uses twikit's "await next" function
        to get more tweets with the given query. The corresponding
        users are then parsed from such tweets.

        Args:
            - query (str): Query with keywords and location
            - account_num (int): Determines which account to use from twikit
        """
        client = twikit.Client("en-US")
        client.load_cookies(TWIKIT_COOKIES_DICT[f"account_num_{self.account_num}"])
        users_list = []

        tweets = await client.search_tweet(
            query, "Latest", count=num_tweets
        )
        # TODO: Add set operation to keep unique users only
        users_list = self.parse_twikit_users(tweets)

        more_tweets_available = True
        num_iter = 1

        next_tweets = await tweets.next()
        if next_tweets:
            next_users_list = self.parse_twikit_users(next_tweets)
            users_list.extend(next_users_list)
        else:
            more_tweets_available = False

        while more_tweets_available:
            next_tweets = await next_tweets.next()
            if next_tweets:
                next_users_list = self.parse_twikit_users(next_tweets)
                users_list.extend(next_users_list)

            else:
                more_tweets_available = False

            if num_iter % 5 == 0:
                print(f"Processed {num_iter} batches")

            # Leave process running until tweets are recollected
            if num_iter == TWIKIT_TWEETS_THRESHOLD:
                time.sleep(FIFTEEN_MINUTES)

            num_iter += 1

        return users_list

    def _get_x_city_users(self, query, num_tweets):
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

    async def _get_city_users(self, num_tweets, query):
        """
        Searches for city users with either twikit or X, then sends them
        to the corresponding queue

        Args:
            - query (str): Query with keywords and the corresponding location
        """
        # TODO: Insert / Check city node
        if self.extraction_type == "twikit":
            users_list = await self._get_twikit_city_users(num_tweets, query)
        elif self.extraction_type == "x":
            users_list = self._get_x_city_users(num_tweets, query)
        elif self.extraction_type == "file":
            users_list = self._get_file_city_users()
            print("Got users from file")

        # TODO: Upload user attributes to Neptune -- Neptune handler class
        # TODO: Create one method to send messages to the queue
        user_tweets_queue_url = self.sqs_client.get_queue_url(
            QueueName="UserTweets"
        )["QueueUrl"]
        # user_followers_queue_url = self.sqs_client.get_queue_url(
        #     QueueName="UserFollowers"
        # )["QueueUrl"]
        for user in users_list:
            # if user["city"] and user["num_followers"] > 100:
                print(f"Sending user {user}")
                message = {
                    "user_id": user,
                    "location": self.main_city,
                }
                try:
                    # Send to user tweets
                    self.sqs_client.send_message(
                            QueueUrl=user_tweets_queue_url,
                            MessageBody=json.dumps(message),
                        )
                    print("Success :)")
                except Exception as err:
                    print(f"Unable to send user {user} to  User Tweets SQS: {err}")
                # Send to user followers
                # try:
                #     self.sqs_client.send_message(
                #         QueueUrl=user_followers_queue_url,
                #         messageBody=json.dumps(message),
                #     )
                # except Exception as err:
                #     print(f"Unable to send user {user['user_id']} to  User Followers SQS: {err}")
                #     continue
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
        choices=["file", "twikit", "x"],
        help="Choose how to get users",
    )
    parser.add_argument(
        "--wait",
        type=str,
        choices=["Yes", "No"],
        help="Decide whether to wait 15 mins or not",
    )
    parser.add_argument(
        "--account_num",
        type=int,
        help="Account number to use with twikit",
    )
    args = parser.parse_args()

    if args.wait == "Yes":
        print("Sleeping for 15 minutes...")
        time.sleep(FIFTEEN_MINUTES)

    city_users = CityUsers(args.location, args.tweet_count, args.extraction_type, args.account_num)
    asyncio.run(city_users._get_city_users(args.tweet_count, query=""))
