"""
Script to search tweets and users from a particular location.
The users who match a certain criteria will be sent to a processing queue.

Author: Federico Dominguez Molina & Vishal Joseph
Last Updated: May 2025
"""

import asyncio
import json
import time
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import botocore
import pandas as pd
import twikit
from config_utils.cities import CITIES_LANGS, LOCATION_ALIAS_DICT
from config_utils.constants import (
    EXPANSIONS,
    FIFTEEN_MINUTES,
    INFLUENCER_FOLLOWERS_THRESHOLD,
    NEPTUNE_S3_BUCKET,
    REGION_NAME,
    SQS_USER_FOLLOWERS,
    SQS_USER_TWEETS,
    THIRTY_DAYS,
    TWEET_FIELDS,
    TWIKIT_COOKIES_DICT,
    USER_FIELDS,
    X_SEARCH_MAX_TWEETS,
    X_SEARCH_MIN_TWEETS,
)
from config_utils.queries import QUERIES_DICT
from config_utils.util import (
    check_location,
    client_creator,
    convert_to_iso_format,
)


class CityUsers:
    def __init__(self, location):
        self.base_dir = Path(__file__).parent / "data/"
        self.location = location
        self.sqs_client = boto3.client("sqs", region_name="us-west-1")
        self.language = CITIES_LANGS[self.location]

    @staticmethod
    def get_default_date_range(date_since=None, date_until=None):
        """
        Gets a default date range where the most
        recent date is today and the last date is
        30 days before
        """
        if not date_until:
            date_until = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if not date_since:
            date_since = datetime.now(timezone.utc) - timedelta(
                days=THIRTY_DAYS
            )
            date_since = date_since.strftime("%Y-%m-%d")

        return date_since, date_until

    def extract_queries_num_tweets(
        self, tweet_count, extraction_type, date_since=None, date_until=None
    ):
        """
        This method gets all the queries with the appropiate aliases
        for the desired location, along with the allocated number of
        tweets for each query / account type

        Args
        ----------
            - tweet_count (str): Total number of tweets to be distributed for each
                         account type
            - date_since (str): Lower bound date (YYYY-MM-DD) for tweet search
            - date_until (str): Upper bound date (YYYY-MM-DD) for tweet search
        """
        queries = QUERIES_DICT[self.language]

        # date_since and date_until just supported in X Enterprise
        if extraction_type in ["twikit"]:
            date_since, date_until = self.get_default_date_range(
                date_since, date_until
            )
            date_range = f"since:{date_since} until:{date_until}"
        else:
            date_range = ""

        if self.location in LOCATION_ALIAS_DICT:
            aliases = LOCATION_ALIAS_DICT[self.location]
        else:
            aliases = [self.location]

        num_tweets_per_account = tweet_count // len(queries)
        new_query_dict = {}

        for account_type in queries:
            query = queries[account_type]
            # Adding combination of all aliases
            aliases_str = " OR ".join(aliases)
            query = query.replace("location", aliases_str)
            query = query.replace("\n", " ").strip()
            query = query.replace("  ", " ")
            query = query.replace("\t", " ")
            query = f"{query} {date_range}"

            new_query_dict[account_type] = query

        return new_query_dict, num_tweets_per_account

    def parse_x_users(self, user_list):
        """
        This function takes a list of user objects and
        transforms it into a list of dictionaries

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
                #  datetime.datetime(2008, 6, 6, 21, 49, 51, tzinfo=datetime.timezone.utc)
                "created_at": user["created_at"].isoformat(),
                "processing_status": "pending",
            }
            for key, value in user["public_metrics"].items():
                user_dict[key] = value

            user_dict["category"] = "null"
            user_dict["treatment_arm"] = "null"
            user_dict["retweeter_status"] = "pending"
            user_dict["retweeter_last_processed"] = "null"
            user_dict["follower_status"] = "pending"
            user_dict["follower_last_processed"] = "null"
            user_dict["extracted_at"] = datetime.now(timezone.utc).isoformat()
            user_dict["last_updated"] = datetime.now(timezone.utc).isoformat()
            # See if location matches to add city
            location_match = check_location(user["location"], self.location)
            user_dict["city"] = self.location if location_match else None
            user_dicts.append(user_dict)

        return user_dicts

    @staticmethod
    def filter_users(user_list):
        """
        Keeps users that:
            - are unique
            - have more than 100 followers
            - match the location

        Args:
            - user_list (list)
        Returns:
            - new_user_list (list)
        """
        new_user_list = []
        unique_ids = []

        for user_dict in user_list:
            user_id = user_dict["user_id"]
            if user_id in unique_ids:
                continue
            elif (
                user_dict["city"]
                and user_dict["followers_count"]
                > INFLUENCER_FOLLOWERS_THRESHOLD
            ):
                unique_ids.append(user_id)
                new_user_list.append(user_dict)
            else:
                continue

        return new_user_list

    def parse_twikit_users(self, tweets):
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
        # TODO: Add logic to only process unique users at a global level
        # Check user attributes in neptune (retweeter_status in progress or completed)
        # If retweeter_status is completed / in progress - skip
        if tweets:
            for tweet in tweets:
                # if tweet.user.id
                user_dict = {}
                user_dict["user_id"] = tweet.user.id
                user_dict["username"] = tweet.user.screen_name
                user_dict["description"] = tweet.user.description
                user_dict["profile_location"] = tweet.user.location
                user_dict["target_location"] = self.location
                user_dict["followers_count"] = tweet.user.followers_count
                user_dict["following_count"] = tweet.user.following_count
                user_dict["tweets_count"] = tweet.user.statuses_count
                user_dict["verified"] = tweet.user.verified
                user_dict["created_at"] = convert_to_iso_format(
                    tweet.user.created_at
                )
                user_dict["category"] = "null"
                user_dict["treatment_arm"] = "null"
                user_dict["retweeter_status"] = "pending"
                user_dict["retweeter_last_processed"] = "null"
                user_dict["follower_status"] = "pending"
                user_dict["follower_last_processed"] = "null"
                user_dict["extracted_at"] = datetime.now(
                    timezone.utc
                ).isoformat()
                user_dict["last_updated"] = datetime.now(
                    timezone.utc
                ).isoformat()
                # See if location matches to add city
                location_match = check_location(
                    tweet.user.location, self.location
                )
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
        client.load_cookies(TWIKIT_COOKIES_DICT[f"account_{account_num}"])

        new_users_list = []
        for user in users_list:
            user_dict = {}
            user_dict["user_id"] = user
            success = False
            while not success:
                # Get source user information
                try:
                    user_obj = await client.get_user_by_id(user_dict["user_id"])
                except twikit.errors.TooManyRequests:
                    print("User Attributes: Too Many Requests...")
                    time.sleep(FIFTEEN_MINUTES)
                    user_obj = await client.get_user_by_id(user_dict["user_id"])
                except twikit.errors.BadRequest:
                    print("User Attributes: Bad Request")
                    continue
                except twikit.errors.NotFound:
                    print("User Attributes: Not Found")
                    continue
                except twikit.errors.TwitterException as err:
                    print(
                        f"User Attributes: Twitter Error ({err} - {user['user_id']})"
                    )
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
                user_dict["created_at"] = convert_to_iso_format(
                    user_obj.created_at
                )
                user_dict["category"] = "null"
                user_dict["treatment_arm"] = "null"
                user_dict["retweeter_status"] = "pending"
                user_dict["retweeter_last_processed"] = "null"
                user_dict["follower_status"] = "pending"
                user_dict["follower_last_processed"] = "null"
                user_dict["extracted_at"] = datetime.now(
                    timezone.utc
                ).isoformat()
                user_dict["last_updated"] = datetime.now(
                    timezone.utc
                ).isoformat()

                # See if location matches to add city
                location_match = check_location(
                    user_obj.location, self.location
                )
                user_dict["city"] = self.location if location_match else None
                new_users_list.append(user_dict)
                success = True

        return new_users_list

    def _get_file_city_users(self, num_users=None):
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
        users_list = (
            self.user_df.loc[:, "user_id"].astype(str).unique().tolist()
        )
        if num_users:
            print(f"Only selecting {num_users}")
            users_list = users_list[:num_users]

        return users_list

    async def _get_twikit_city_users(
        self, queries_dict, num_tweets, account_num
    ):
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
        cookies_dir = TWIKIT_COOKIES_DICT[f"account_{account_num}"]
        cookies_dir = Path(__file__).parent.parent / cookies_dir
        client.load_cookies(cookies_dir)
        users_list = []
        num_iter = 0

        for account_type, query in queries_dict.items():
            num_extracted_tweets = 0
            print(
                f" =============== PROCESSING: {account_type} ======================"
            )
            try:
                tweets = await client.search_tweet(
                    query, "Latest", count=num_tweets
                )
                print(f"First request, got : {len(tweets)} tweets")
                print(tweets)
                num_extracted_tweets += len(tweets)
            except twikit.errors.TooManyRequests:
                print("Tweets: Too Many Requests...")
                time.sleep(FIFTEEN_MINUTES)
                tweets = await client.search_tweet(
                    query, "Latest", count=num_tweets
                )
                num_extracted_tweets += len(tweets)
            parsed_users = self.parse_twikit_users(tweets)
            users_list.extend(parsed_users)

            while num_extracted_tweets < num_tweets:
                num_iter += 1
                try:
                    if num_iter == 1:
                        next_tweets = await tweets.next()
                    else:
                        next_tweets = await next_tweets.next()

                    if next_tweets:
                        # Just getting the exactly necessary tweets - It will be more than required!
                        next_users_list = self.parse_twikit_users(next_tweets)
                        users_list.extend(next_users_list)
                        num_extracted_tweets += len(next_tweets)
                        print(
                            f"Request {num_iter}, got : {len(next_tweets)} tweets"
                        )
                        print(next_tweets)
                    else:
                        print("No more tweets, moving on to next query")
                        break
                except twikit.errors.TooManyRequests:
                    print("Tweets: too many requests, sleeping...")
                    time.sleep(FIFTEEN_MINUTES)
                if num_iter % 5 == 0:
                    print(f"Processed {num_iter} batches")
                # Leave process running until tweets are recollected

        return users_list

    def _get_x_city_users(
        self, queries_dict, num_tweets, date_since=None, date_until=None
    ):
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

        # Gets default date range if necessary
        date_since, date_until = self.get_default_date_range(
            date_since, date_until
        )

        for account_type, query in queries_dict.items():
            print(query)
            print(
                f"=============== Processing {account_type} =================="
            )
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
                    start_time=date_since,
                    end_time=date_until,
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

            print(f"Extracted len({tweets_list})")
            print(tweets_list)

        return users_list

    def send_to_queue(self, users_list, queue_name):
        """
        Sends twikit or X users to the corresponding queue

        Args:
            - users_list (list)
            - queue_name (str)
        """
        queue_url = self.sqs_client.get_queue_url(QueueName=queue_name)[
            "QueueUrl"
        ]
        if not users_list:
            print("No users to send to queue")
            return
        for user in users_list:
            print(f"Sending user {user['user_id']}")
            message = {
                "user_id": user["user_id"],
                "location": self.location,
            }
            try:
                self.sqs_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(message),
                )
                print(f"User {user['user_id']} sent to {queue_name} queue :)")
            except Exception as err:
                print(
                    f"Unable to send user {user['user_id']} to  {queue_name} SQS: {err}"
                )

    def insert_descriptions_to_s3(self, users_list):
        """
        Function to insert each user's description as
        a txt file to S3
        """
        s3_client = boto3.client("s3", region_name=REGION_NAME)
        for user in users_list:
            s3_path = f"networks/{self.location}/classification/{user['user_id']}/input/description.txt"
            try:
                s3_client.put_object(
                    Bucket=NEPTUNE_S3_BUCKET,
                    Key=s3_path,
                    Body=user["description"].encode("utf-8", errors="ignore"),
                )
            except botocore.exceptions.ClientError:
                print(f"Unable to upload description for {user['user_id']}")
                continue


if __name__ == "__main__":
    parser = ArgumentParser(
        "Parameters to get users data to generate a network"
    )
    parser.add_argument(
        "--location", type=str, help="Location to search users from"
    )
    parser.add_argument(
        "--tweet_count", type=int, help="Number of tweets to get"
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
    parser.add_argument(
        "--date_since",
        type=str,
        help="Lower bound date to get tweets",
    )
    parser.add_argument(
        "--date_until",
        type=str,
        help="Upper bound date to get the tweets",
    )

    args = parser.parse_args()

    if args.extraction_type == "X" and (
        (args.tweet_count < X_SEARCH_MIN_TWEETS)
        or (args.tweet_count > X_SEARCH_MAX_TWEETS)
    ):
        raise ValueError(
            f"Number of tweets for X extraction should be {X_SEARCH_MIN_TWEETS}< number < {X_SEARCH_MAX_TWEETS}"
        )

    city_users = CityUsers(args.location)

    # Getting dict queries and num of tweets per account
    if args.extraction_type in ["twikit", "X"]:
        if args.date_since and args.date_until:
            new_query_dict, num_tweets_per_account = (
                city_users.extract_queries_num_tweets(
                    args.tweet_count,
                    args.extraction_type,
                    args.date_since,
                    args.date_until,
                )
            )
        else:
            new_query_dict, num_tweets_per_account = (
                city_users.extract_queries_num_tweets(
                    args.tweet_count, args.extraction_type
                )
            )

    if args.extraction_type == "twikit":
        print(f"Number of tweets per account: {num_tweets_per_account}")
        users_list = asyncio.run(
            city_users._get_twikit_city_users(
                new_query_dict, num_tweets_per_account, args.account_num
            )
        )
    elif args.extraction_type == "X":
        new_query_dict, num_tweets_per_account = (
            city_users.extract_queries_num_tweets(
                args.tweet_count, args.extraction_type
            )
        )
        users_list = city_users._get_x_city_users(
            new_query_dict, num_tweets_per_account
        )
    elif args.extraction_type == "file":
        users_list = city_users._get_file_city_users(args.num_users)
        print("Got users from file")
        users_list = asyncio.run(
            city_users.get_user_attributes(users_list, args.account_num)
        )
        print("Got user attributes")

    print(
        f" =========================== Before filtering ==================:\n {len(users_list)} users"
    )
    users_list = city_users.filter_users(users_list)
    print(
        f" =========================== After filtering ==================:\n {len(users_list)} users"
    )
    print("\n")

    # TODO: Insert / Check city node

    # TODO: Upload user attributes to Neptune -- Neptune handler class

    city_users.insert_descriptions_to_s3(users_list)

    print("Sending to UserTweets Queue")
    city_users.send_to_queue(users_list, SQS_USER_TWEETS)
    print("Sending to UserFollowers Queue")
    city_users.send_to_queue(users_list, SQS_USER_FOLLOWERS)

    # TODO: "follower_status": pending, queued, in_progress, completed, failed
    # here, follower_status will be set as queued
