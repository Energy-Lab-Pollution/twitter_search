"""
Script to get a determined user's followers
"""

import asyncio
import json
import time
from argparse import ArgumentParser
from datetime import datetime, timezone

import boto3
import tweepy
import twikit
from config_utils.constants import (
    FIFTEEN_MINUTES,
    INFLUENCER_FOLLOWERS_THRESHOLD,
    REGION_NAME,
    SQS_USER_FOLLOWERS,
    SQS_USER_TWEETS,
    TWIKIT_COOKIES_DICT,
)
from config_utils.util import (
    api_v1_creator,
    check_location,
    convert_to_iso_format,
)


SQS_CLIENT = boto3.client("sqs", region_name=REGION_NAME)


class UserFollowers:

    def __init__(self, location):
        self.location = location

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
                user_dict["extracted_at"] = datetime.now(
                    timezone.utc
                ).isoformat()
                user_dict["last_updated"] = datetime.now(
                    timezone.utc
                ).isoformat()
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
        client.load_cookies(TWIKIT_COOKIES_DICT[f"account_{account_num}"])

        while extracted_followers < follower_count:
            try:
                # try to fetch
                followers = await client.get_user_followers(
                    user_id, count=follower_count
                )
            except twikit.errors.NotFound as error:
                print(f"Followers: Not Found - {error}")
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
            api_v1_client.get_followers, user_id=user_id, count=follower_count
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

    def send_to_queue(self, users_list, queue_name):
        """
        Sends twikit or X users to the corresponding queue

        Args:
            - users_list (list)
            - queue_name (str)
        """
        queue_url = SQS_CLIENT.get_queue_url(QueueName=queue_name)["QueueUrl"]
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

    parser.add_argument(
        "--further_extraction",
        type=str,
        choices=["True", "False"],
        help="Decide if we will get retweeters and followers for extracted users",
    )

    args = parser.parse_args()
    further_extraction = True if args.further_extraction is True else False
    user_followers_queue_url = SQS_CLIENT.get_queue_url(
        QueueName=SQS_USER_FOLLOWERS
    )["QueueUrl"]

    while True:
        # Pass Queue Name and get its URL
        response = SQS_CLIENT.receive_message(
            QueueUrl=user_followers_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
        )
        try:
            message = response["Messages"][0]
            receipt_handle = message["ReceiptHandle"]
            clean_data = json.loads(message["Body"])

        except KeyError:
            # Empty queue
            print("Empty queue")
            continue

        # Getting information from body message
        print(clean_data)
        root_user_id = str(clean_data["user_id"])
        location = clean_data["location"]

        user_followers = UserFollowers(location)

        if args.extraction_type == "twikit":
            followers_list = asyncio.run(
                user_followers.twikit_get_followers(
                    user_id=root_user_id,
                    follower_count=args.num_followers,
                    account_num=args.account_num,
                )
            )
        elif args.extraction_type == "X":
            raise Exception("X API Followers endpoint is only supported for Enterprise")
            # followers_list = user_followers.x_get_followers(
            #     user_id=root_user_id, follower_count=args.num_followers
            # )

        print(f"Got {len(followers_list)} for {root_user_id} before filtering")
        followers_list = user_followers.filter_users(followers_list)
        print(f"Got {len(followers_list)} for {root_user_id} after filtering")

        # TODO: Check if users exist on neptune

        # Send users to ser tweets queue
        if further_extraction:
            print("Getting followers' information...")
            user_followers.send_to_queue(
                followers_list, user_id=root_user_id, queue_name=SQS_USER_TWEETS
            )

        # TODO: "follower_status": pending, queued, in_progress, "completed", "failed

        # Delete root user message from queue so it is not picked up again
        SQS_CLIENT.delete_message(
            QueueUrl=user_followers_queue_url,
            ReceiptHandle=receipt_handle,
        )
