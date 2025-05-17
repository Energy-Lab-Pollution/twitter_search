"""
Script to get a determined user's followers
"""

import asyncio
from datetime import datetime, timezone
import json
import time
from argparse import ArgumentParser

import boto3
import botocore
import tweepy
import twikit

from config_utils.constants import (
    FIFTEEN_MINUTES,
    REGION_NAME,
    SQS_USER_FOLLOWERS,    
    TWIKIT_COOKIES_DICT,
)
from config_utils.util import (
    check_location,
    api_v1_creator,
    convert_to_iso_format,
)


SQS_CLIENT = boto3.client("sqs", region=REGION_NAME)


class UserFollowers:

    def __init__(self, location):
        self.location = location

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
            # Followers and retweeters status
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
                user_dict["extracted_at"] = datetime.now(timezone.utc).isoformat()
                user_dict["last_updated"] = datetime.now(timezone.utc).isoformat()
                # See if location matches to add city
                location_match = check_location(user.location, self.location)
                user_dict["city"] = self.location if location_match else None
                users_list.append(user_dict)

        return users_list

    async def twikit_get_followers(self, user_id, follower_count, account_num):
        """
        Gets a given user's followers

        Args:
        ---------
            - user_id (str): User_id of user to look for
        Returns:
        ---------
            - tweet_list(list): List of dicts with followers info
        """
        followers_list = []
        num_iter = 0
        extracted_followers = 0
        client = twikit.Client("en-US")
        client.load_cookies(
            TWIKIT_COOKIES_DICT[f"account_{account_num}"]
        )

        while extracted_followers < follower_count:
            try:
                # try to fetch
                followers = await client.get_user_followers(
                    user_id, count=follower_count
                )
            except twikit.errors.NotFound as error:
                print("Followers: Not Found")
                return followers_list
            except twikit.errors.TooManyRequests:
                print("Followers: Too Many Requests - stopping early")
                time.sleep(FIFTEEN_MINUTES)
            except twikit.errors.BadRequest:
                print("Followers: Bad Request - stopping early")
                return followers_list
            except twikit.errors.TwitterException as e:
                print(f"Followers: Twitter Exception - {e}")
                return followers_list

            parsed_followers = self.parse_twikit_users(followers)
            followers_list.extend(parsed_followers)
            extracted_followers += len(parsed_followers)
            num_iter += 1
            try:
                if num_iter == 1:
                    more_followers = await followers.next()
                else:
                    more_followers = await more_followers.next()
                if more_followers:
                    more_parsed_followers = self.parse_twikit_users(
                        more_followers
                    )
                    followers_list.extend(more_parsed_followers)
                    extracted_followers += len(more_parsed_followers)
            # Stop here and just return what you got
            except twikit.errors.TooManyRequests:
                print("Followers: too many requests...")
                time.sleep(FIFTEEN_MINUTES)
            except twikit.errors.BadRequest:
                print("Followers: Bad Request")
                return followers_list
            except twikit.errors.NotFound:
                print("Followers: Not Found")
                return followers_list
            except twikit.errors.TwitterException as e:
                print(f"Followers: Twitter Exception {e}")
                return followers_list
            if num_iter % 5 == 0:
                print(f"Processed {num_iter} follower batches, sleeping...")
                time.sleep(1)

        # TODO: Upload users data to DynamoDB - store_data('user')
        # TODO: Send to SQS for network processing
        return followers_list

    def x_get_followers(self, user_id, follower_count):
        """
        Pull up to 1 000 followers via v1.1, convert each to a dict
        shape that parse_x_users expects, then parse.
        """
        api_v1_client = api_v1_creator()
        legacy_users = tweepy.Cursor(
            api_v1_client.followers, user_id=user_id, count=follower_count
        ).items(follower_count)

        # Convert to dicts
        normalized = []
        for legacy_user in legacy_users:
            normalized.append(
                {
                    "id": legacy_user.id_str,
                    "username": legacy_user.screen_name,
                    "description": legacy_user.description,
                    "location": legacy_user.location,
                    "created_at": legacy_user.created_at.isoformat(),
                    "verified": legacy_user.verified,
                    "public_metrics": {
                        "followers_count": legacy_user.followers_count,
                        "following_count": legacy_user.friends_count,
                        "tweet_count": legacy_user.statuses_count,
                        "listed_count": legacy_user.listed_count,
                    },
                }
            )

        return self.parse_x_users(normalized)
    
if __name__ == "__main__":
    parser = ArgumentParser(
        "Parameters to get users data to generate a network"
    )
    parser.add_argument(
        "--num_followers", type=int, help="Number of tweets to get"
    )
    parser.add_argument(
        "--extraction_type",
        type=str,
        choices=["twikit", "X"],
        help="Choose how to get user's tweets",
    )
    parser.add_argument(
        "--account_num",
        type=int,
        help="Account number to use with twikit",
    )

    args = parser.parse_args()
    
    tweet_retweeters = UserFollowers(args.location)
    user_tweets_queue_url = SQS_CLIENT.get_queue_url(QueueName=SQS_USER_FOLLOWERS)["QueueUrl"]

    while True:
        # Pass Queue Name and get its URL
        response = SQS_CLIENT.receive_message(
            QueueUrl=user_tweets_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
        )
        try:
            message = response["Messages"][0]
            receipt_handle = message["ReceiptHandle"]
            data = json.loads(message["Body"])

        except KeyError:
            # Empty queue
            print("Empty queue")
            continue

        # Getting information from body message
        data = data["Message"]
        clean_data = json.loads(data)