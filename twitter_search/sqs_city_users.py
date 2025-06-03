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
    NEPTUNE_ENDPOINT,
    NEPTUNE_S3_BUCKET,
    REGION_NAME,
    SQS_USER_FOLLOWERS,
    SQS_USER_TWEETS,
    TWEET_FIELDS,
    TWIKIT_COOKIES_DICT,
    USER_FIELDS,
    X_SEARCH_MAX_TWEETS,
    X_SEARCH_MIN_TWEETS,
)
from config_utils.neptune_handler import NeptuneHandler
from config_utils.queries import QUERIES_DICT
from config_utils.util import (
    check_location,
    client_creator,
    convert_to_iso_format,
)


class CityUsers:
    def __init__(self, location, neptune_handler):
        self.base_dir = Path(__file__).parent / "data/"
        self.location = location
        self.sqs_client = boto3.client("sqs", region_name="us-west-1")
        self.language = CITIES_LANGS.get(self.location, None)
        self.neptune_handler = neptune_handler

    def extract_queries_num_tweets(self, tweet_count, date_range):
        """
        This method gets all the queries with the appropiate aliases
        for the desired location, along with the allocated number of
        tweets for each query / account type

        Args
        ----------
            - tweet_count (str): Total number of tweets to be distributed for each
                         account type
            - date_range (str): Date range for tweet search.
        """
        queries = QUERIES_DICT[self.language]

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
        transforms it into a dictionary of dictionaries

        Parameters
        ----------
        user_list : list
            List of user objects

        Returns
        -------
        user_dicts: dict
            Dict of dictionaries with user data
        """
        user_dicts = {}
        for user in user_list:
            if user["id"] not in user_dicts:
                user_dict = {
                    "user_id": user["id"],
                    "username": user["username"],
                    "description": user["description"],
                    "profile_location": user["location"],
                    "target_location": self.location,
                    "verified": "true" if user["verified"] else "false",
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
                user_dict["extracted_at"] = datetime.now(
                    timezone.utc
                ).isoformat()
                user_dict["last_updated"] = datetime.now(
                    timezone.utc
                ).isoformat()
                # See if location matches to add city
                location_match = check_location(user["location"], self.location)
                user_dict["city"] = self.location if location_match else None
                # Append to master dict
                user_dicts[user["id"]] = user_dict

        return user_dicts

    def parse_twikit_users(self, tweets):
        """
        Parse retweeters (user objects) and put them
        into a dict of dictionaries.

        Args:
        ----------
            - tweets (list): list of twikit.User objects
        Returns:
        ----------
            - user_dicts (dict): Dict of dictionaries with users' info
        """
        user_dicts = {}
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
                user_dict["verified"] = (
                    "true" if tweet.user.verified else "false"
                )
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

                # Append to master dict
                user_dicts[tweet.user.id] = user_dict

        return user_dicts

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
        self, tweet_count, date_since, date_until, account_num
    ):
        """
        Method used to search for tweets, with twikit,
        using a given query

        This method uses twikit's "await next" function
        to get more tweets with the given query. The corresponding
        users are then parsed from such tweets.

        Args:
            - tweet_count (int): Total number of tweets to extract/search
            - date_since (str): Start date for tweet search
            - date_until (str): End date for tweet search
            - account_num (str): Twitter account to use for extraction
        """
        client = twikit.Client("en-US")
        cookies_dir = TWIKIT_COOKIES_DICT[f"account_{account_num}"]
        cookies_dir = Path(__file__).parent.parent / cookies_dir
        client.load_cookies(cookies_dir)
        users_dict = {}
        num_iter = 0

        date_range = f"since:{date_since} until:{date_until}"
        queries_dict, num_tweets = self.extract_queries_num_tweets(
            tweet_count, date_range
        )
        print(f"Number of tweets per account: {num_tweets}")

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
            users_dict = users_dict | parsed_users

            while num_extracted_tweets < num_tweets:
                num_iter += 1
                try:
                    if num_iter == 1:
                        next_tweets = await tweets.next()
                    else:
                        next_tweets = await next_tweets.next()

                    if next_tweets:
                        # Just getting the exactly necessary tweets - It will be more than required!
                        next_users = self.parse_twikit_users(next_tweets)
                        users_dict = users_dict | next_users
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

        return list(users_dict.values())

    def _get_x_city_users(self, tweet_count, date_since, date_until):
        """
        Method used to search for tweets, with the X API,
        using a given query

        The corresponding users are then parsed from
        such tweets.

        Args:
            - tweet_count (int): Total number of tweets to extract/search
            - date_since (str): Start date for tweet search
            - date_until (str): End date for tweet search
        """
        x_client = client_creator()
        result_count = 0
        next_token = None
        users_dict = {}
        tweets_list = []

        date_range = ""
        queries_dict, num_tweets = self.extract_queries_num_tweets(
            tweet_count, date_range
        )

        # validate tweet count threshold
        self.validate_x_tweet_count(num_tweets)

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
                parsed_users = self.parse_x_users(response.includes["users"])
                users_dict = users_dict | parsed_users
                try:
                    next_token = response.meta["next_token"]
                except Exception as err:
                    print(err)
                    next_token = None

                if next_token is None:
                    break

            print(f"Extracted len({tweets_list})")
            print(tweets_list)

        return list(users_dict.values())

    @staticmethod
    def validate_x_tweet_count(tweet_count):
        """
        Check whether tweet count is within prescribed X API limits.

        Args:
            - tweet_count (int)
        """
        if (tweet_count < X_SEARCH_MIN_TWEETS) or (
            tweet_count > X_SEARCH_MAX_TWEETS
        ):
            raise ValueError(
                f"Number of tweets for X extraction should be {X_SEARCH_MIN_TWEETS}< number < {X_SEARCH_MAX_TWEETS}"
            )

    def send_to_queue(self, user_dict, queue_name):
        """
        Sends twikit or X users to the corresponding queue

        Args:
            - user_dict (dict)
            - queue_name (str)
        """
        queue_url = self.sqs_client.get_queue_url(QueueName=queue_name)[
            "QueueUrl"
        ]
        print(f"Sending user {user_dict['user_id']}")
        message = {
            "user_id": user_dict["user_id"],
            "location": self.location,
        }
        try:
            self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message),
            )
            print(f"User {user_dict['user_id']} sent to {queue_name} queue :)")
        except Exception as err:
            print(
                f"Unable to send user {user_dict['user_id']} to  {queue_name} SQS: {err}"
            )

    def insert_description_to_s3(self, user_dict):
        """
        Function to insert each user's description as
        a txt file to S3
        """
        s3_client = boto3.client("s3", region_name=REGION_NAME)
        s3_path = f"networks/{self.location}/classification/{user_dict['user_id']}/input/description.txt"
        try:
            s3_client.put_object(
                Bucket=NEPTUNE_S3_BUCKET,
                Key=s3_path,
                Body=user_dict["description"].encode("utf-8", errors="ignore"),
            )
        except botocore.exceptions.ClientError:
            print(f"Unable to upload description for {user_dict['user_id']}")

    def validate_root_user(self, user_dict):
        """
        Keeps users that:
            - match the location
            - have more than 100 followers
            - do not already exist in the Graph DB
        Args:
            - user_dict (dict)
        Returns:
            - status (bool)
        """
        user_exists = neptune_handler.user_exists(user_dict["user_id"])
        if (
            not user_exists
            and (user_dict["city"] == user_dict["target_location"])
            and user_dict["followers_count"] > INFLUENCER_FOLLOWERS_THRESHOLD
        ):
            return True
        return False

    def process_and_dispatch_users(self, users_list):
        """
        Validate root users, write nodes/edges to the DB and send to
        SQS for extraction.

        Args:
        ----------
            - users_list (list): list of user dicts
        """
        # Start Neptune client
        self.neptune_handler.start()

        # Check if city node exists
        city_exists = neptune_handler.city_exists(self.location)
        if not city_exists:
            raise ValueError(
                "City node must exist prior to storing additional information"
            )

        for user_dict in users_list:
            print("Validating root user")
            validation_status = self.validate_root_user(user_dict)
            if not validation_status:
                print("Root user validation failed. Skipping...")
                continue

            print(f"{user_dict['user_id']} has been validated as a root user")

            print(
                "Creating root user node and corresponding city edge if applicable"
            )
            self.neptune_handler.create_user_node(user_dict)

            print("Inserting user description")
            self.insert_description_to_s3(user_dict)
            print("Sending to UserTweets Queue")
            self.send_to_queue(user_dict, SQS_USER_TWEETS)
            print("Sending to UserFollowers Queue")
            self.send_to_queue(user_dict, SQS_USER_FOLLOWERS)

            print("Updating follower_status attribute")
            props_dict = {"follower_status": "queued"}
            self.neptune_handler.update_node_attributes(
                label="User",
                node_id=user_dict["user_id"],
                props_dict=props_dict,
            )

        # Stop Neptune client
        self.neptune_handler.stop()


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

    if (not args.date_since) and args.date_until:
        raise ValueError(
            "date_since must be provided if date_until is specified"
        )

    if not args.date_until:
        date_until = (
            datetime.now(timezone.utc) - timedelta(seconds=60)
        ).isoformat()
    else:
        date_until = args.date_until

    if not args.date_since:
        date_since = (
            datetime.now(timezone.utc) - timedelta(days=6)
        ).isoformat()
    else:
        date_since = args.date_since

    neptune_handler = NeptuneHandler(NEPTUNE_ENDPOINT)
    city_users = CityUsers(args.location, neptune_handler)

    if args.extraction_type == "twikit":
        users_list = asyncio.run(
            city_users._get_twikit_city_users(
                args.tweet_count, date_since, date_until, args.account_num
            )
        )
    elif args.extraction_type == "X":
        users_list = city_users._get_x_city_users(
            args.tweet_count, date_since, date_until
        )
    elif args.extraction_type == "file":
        users_list = city_users._get_file_city_users(args.num_users)
        print("Got users from file")
        users_list = asyncio.run(
            city_users.get_user_attributes(users_list, args.account_num)
        )
        print("Got user attributes")

    # users_list = [
    #     {
    #     "user_id": '00123456',
    #     "city": 'test',
    #     "username": "1arry1iu",
    #     "last_tweeted_at": "2025-05-02T20:47:42+00:00",
    #     "retweeter_status": "pending",
    #     "retweeter_last_processed": "2025-05-02T22:13:46.742222+00:00",
    #     "profile_location": "Chiang Mai, Thailand",
    #     "follower_status": "pending",
    #     "target_location": "test",
    #     "follower_last_processed": "2025-05-02T22:13:46.742222+00:00",
    #     "followers_count": 278,
    #     "following_count": 80,
    #     "tweets_count": 444,
    #     "verified": "null",
    #     "created_at": "2022-02-04T00:01:49+00:00",
    #     "category": "null",
    #     "treatment_arm": "null",
    #     "extracted_at": "2025-05-02T22:13:11.834158+00:00",
    #     "last_updated": "2025-05-02T22:13:11.834168+00:00",
    #     }
    # ]

    city_users.process_and_dispatch_users(users_list)
