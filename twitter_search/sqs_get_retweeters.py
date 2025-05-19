"""
Script to get a determined tweet's retweeters
"""

import asyncio
import json
import time
from argparse import ArgumentParser
from datetime import datetime, timezone

import boto3
import twikit
from config_utils.constants import (
    FIFTEEN_MINUTES,
    INFLUENCER_FOLLOWERS_THRESHOLD,
    REGION_NAME,
    SQS_USER_RETWEETERS,
    SQS_USER_TWEETS,
    TWIKIT_COOKIES_DICT,
)
from config_utils.util import (
    check_location,
    client_creator,
    convert_to_iso_format,
)


SQS_CLIENT = boto3.client("sqs", region_name=REGION_NAME)


class UserRetweeters:

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

    async def get_single_tweet_retweeters(
        self, tweet_id, num_retweeters, account_num
    ):
        """
        For a particular tweet, get all the possible retweeters

        Args:
        ---------
            - tweet_id (str): String with tweet id
        Returns:
        ---------
            - retweeters_list (list): List with retweeters info
        """
        client = twikit.Client("en-US")
        client.load_cookies(TWIKIT_COOKIES_DICT[f"account_{account_num}"])

        retweeters_list = []
        extracted_retweeters = 0
        attempt_number = 1

        # Maxed out retweeters threshold
        try:
            retweeters = await client.get_retweeters(
                tweet_id, count=num_retweeters
            )
            if retweeters:
                parsed_retweeters = self.parse_twikit_users(retweeters)
                retweeters_list.extend(parsed_retweeters)
                extracted_retweeters += len(retweeters_list)
        except twikit.errors.TooManyRequests:
            print("Retweeters: Too Many Requests")
            time.sleep(FIFTEEN_MINUTES)
            return None

        while extracted_retweeters < num_retweeters:
            try:
                if attempt_number == 1:
                    more_retweeters = await retweeters.next()
                else:
                    more_retweeters = await more_retweeters.next()
            # Stop here if failure and return what you had so far
            except twikit.errors.TooManyRequests:
                print("Retweeters: Too Many Requests")
                time.sleep(FIFTEEN_MINUTES)
            except twikit.errors.BadRequest:
                print("Retweeters: Bad Request")
                return retweeters_list
            except twikit.errors.TwitterException as e:
                print(f"Retweeters: Twitter Exception {e}")
                return retweeters_list
            if more_retweeters:
                more_parsed_retweeters = self.parse_twikit_users(
                    more_retweeters
                )
                retweeters_list.extend(more_parsed_retweeters)
                extracted_retweeters += len(more_parsed_retweeters)
            else:
                print("No more retweeters available")
                break

        return retweeters_list

    def x_get_single_tweet_retweeters(self, tweet_id, num_retweeters):
        """
        Pull up to 500 retweeters via v2, then parse.
        """
        retweeters_list = []
        next_token = None
        x_client = client_creator()

        while len(retweeters_list) < num_retweeters:
            response = x_client.get_retweeters(
                id=tweet_id,
                max_results=num_retweeters,
                pagination_token=next_token,
                user_fields=[
                    "id",
                    "username",
                    "description",
                    "location",
                    "public_metrics",
                    "verified",
                    "created_at",
                ],
            )

            retweeters = (
                [user.data for user in response.data] if response.data else []
            )
            if not retweeters:
                break
            retweeters_list.extend(retweeters)

            if len(retweeters) >= num_retweeters:
                break

            next_token = response.meta.get("next_token")
            if not next_token:
                break

        return self.parse_x_users(retweeters)

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
                SQS_CLIENT.send_message(
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
        "--num_retweeters", type=int, help="Number of tweets to get"
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
    if args.further_extraction:
        further_extraction = True if args.further_extraction == "True" else False
    else:
        further_extraction = False
    user_retweeters_queue_url = SQS_CLIENT.get_queue_url(
        QueueName=SQS_USER_RETWEETERS
    )["QueueUrl"]

    while True:
        # Pass Queue Name and get its URL
        response = SQS_CLIENT.receive_message(
            QueueUrl=user_retweeters_queue_url,
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
        tweet_id = str(clean_data["tweet_id"])
        target_user_id = str(clean_data["target_user_id"])
        location = clean_data["location"]

        user_retweeters = UserRetweeters(location)

        if args.extraction_type == "twikit":
            user_retweeters_list = asyncio.run(
                user_retweeters.get_single_tweet_retweeters(
                    tweet_id=tweet_id,
                    num_retweeters=args.num_retweeters,
                    account_num=args.account_num,
                )
            )
        elif args.extraction_type == "X":
            user_retweeters_list = user_retweeters.x_get_single_tweet_retweeters(
                tweet_id=tweet_id, num_retweeters=args.num_retweeters
            )

        # TODO: Check if users exist on neptune

        # Send users to ser tweets queue
        if further_extraction:
            print("Getting retweeters' information...")
            user_retweeters_list = user_retweeters.filter_users(user_retweeters_list)
            user_retweeters.send_to_queue(
                user_retweeters_list, queue_name=SQS_USER_TWEETS
            )

        # TODO: "retweeter_status": pending, queued, in_progress, "completed", "failed

        # Delete root user message from queue so it is not picked up again
        SQS_CLIENT.delete_message(
            QueueUrl=user_retweeters_queue_url,
            ReceiptHandle=receipt_handle,
        )
