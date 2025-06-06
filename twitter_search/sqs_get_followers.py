"""
Script to get a determined user's followers
"""

import asyncio
import json
import time
from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path

import boto3
import tweepy
import twikit
from config_utils.constants import (
    FIFTEEN_MINUTES,
    INFLUENCER_FOLLOWERS_THRESHOLD,
    NEPTUNE_ENDPOINT,
    SQS_USER_FOLLOWERS,
    SQS_USER_TWEETS,
    TWIKIT_COOKIES_DICT,
)
from config_utils.neptune_handler import NeptuneHandler
from config_utils.util import (
    api_v1_creator,
    check_location,
    convert_to_iso_format,
)


class UserFollowers:
    def __init__(
        self,
        user_id,
        location,
        further_extraction,
        sqs_client,
        receipt_handle,
        neptune_handler,
    ):
        self.user_id = user_id
        self.location = location
        self.further_extraction = further_extraction
        self.sqs_client = sqs_client
        self.receipt_handle = receipt_handle
        self.neptune_handler = neptune_handler

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
                "verified": "true" if user["verified"] else "false",
                "created_at": user["created_at"],
            }

            user_dict["followers_count"] = user["public_metrics"].get(
                "followers_count", -99
            )
            user_dict["following_count"] = user["public_metrics"].get(
                "following_count", -99
            )
            user_dict["tweets_count"] = user["public_metrics"].get(
                "tweet_count", -99
            )
            user_dict["category"] = "null"
            user_dict["treatment_arm"] = "null"
            # Followers and retweeters status
            user_dict["retweeter_status"] = "pending"
            user_dict["retweeter_last_processed"] = "null"
            user_dict["follower_status"] = "pending"
            user_dict["follower_last_processed"] = "null"
            user_dict["last_tweeted_at"] = "null"
            user_dict["extracted_at"] = datetime.now(timezone.utc).isoformat()
            user_dict["last_updated"] = datetime.now(timezone.utc).isoformat()
            # See if location matches to add city
            location_match = check_location(user["location"], self.location)
            user_dict["city"] = self.location if location_match else None
            user_dicts.append(user_dict)

        return user_dicts

    def parse_twikit_users(self, users):
        """
        Parse followers (user objects) and put them
        into a dict of dictionaries

        Args:
        ----------
            - users (list): list of User objects
        Returns:
        ----------
            - users_dict (dict): dict of dictionaries with users' info
        """
        users_dict = {}

        if users:
            for user in users:
                if user.id in users_dict:
                    continue
                user_dict = {}
                user_dict["user_id"] = user.id
                user_dict["username"] = user.screen_name
                user_dict["description"] = user.description
                user_dict["profile_location"] = user.location
                user_dict["target_location"] = self.location
                user_dict["followers_count"] = user.followers_count
                user_dict["following_count"] = user.following_count
                user_dict["tweets_count"] = user.statuses_count
                user_dict["verified"] = "true" if user.verified else "false"
                user_dict["created_at"] = convert_to_iso_format(user.created_at)
                user_dict["category"] = "null"
                user_dict["treatment_arm"] = "null"
                user_dict["retweeter_status"] = "pending"
                user_dict["retweeter_last_processed"] = "null"
                user_dict["follower_status"] = "pending"
                user_dict["follower_last_processed"] = "null"
                user_dict["last_tweeted_at"] = "null"
                user_dict["extracted_at"] = datetime.now(
                    timezone.utc
                ).isoformat()
                user_dict["last_updated"] = datetime.now(
                    timezone.utc
                ).isoformat()
                # See if location matches to add city
                location_match = check_location(user.location, self.location)
                user_dict["city"] = self.location if location_match else None
                users_dict[user.id] = user_dict

        return users_dict

    async def twikit_get_followers(self, follower_count, account_num):
        """
        Gets a given user's followers

        Args:
        ---------
            - follower_count (int)
            - account_num (int)
        Returns:
        ---------
            - followers_list(list): List of dicts with followers info
        """
        followers_dict = {}
        queue_url = self.sqs_client.get_queue_url(QueueName=SQS_USER_FOLLOWERS)[
            "QueueUrl"
        ]
        num_iter = 0
        extracted_followers = 0
        client = twikit.Client("en-US")
        cookies_dir = TWIKIT_COOKIES_DICT[f"account_{account_num}"]
        cookies_dir = Path(__file__).parent.parent / cookies_dir
        client.load_cookies(cookies_dir)

        flag = False
        for _ in range(3):
            try:
                # try to fetch
                followers = await client.get_user_followers(
                    self.user_id, count=follower_count
                )
                if not followers:
                    print("API call was successful but no output extracted")
                    return []
                flag = True
                parsed_followers = self.parse_twikit_users(followers)
                followers_dict = followers_dict | parsed_followers
                extracted_followers += len(parsed_followers)
                num_iter += 1
                break
            except twikit.errors.NotFound as error:
                print(f"Followers: Not Found - {error}")
                continue
            except twikit.errors.TooManyRequests:
                print("Followers: Too Many Requests")
                self.sqs_client.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=self.receipt_handle,
                    VisibilityTimeout=FIFTEEN_MINUTES,
                )
                time.sleep(FIFTEEN_MINUTES)
                continue
            except twikit.errors.BadRequest:
                print("Followers: Bad Request - stopping early")
                continue
            except twikit.errors.TwitterException as e:
                print(f"Followers: Twitter Exception - {e}")
                continue

        if not flag:
            print("No followers extracted despite 3 retry attempts")
            return []

        while extracted_followers < follower_count:
            try:
                if num_iter == 1:
                    more_followers = await followers.next()
                else:
                    more_followers = await more_followers.next()
                if more_followers:
                    more_parsed_followers = self.parse_twikit_users(
                        more_followers
                    )
                    followers_dict = followers_dict | more_parsed_followers
                    extracted_followers += len(more_parsed_followers)
                    num_iter += 1
                else:
                    print("No more followers, moving on...")
                    break
            except twikit.errors.TooManyRequests:
                print("Followers: too many requests...")
                self.sqs_client.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=self.receipt_handle,
                    VisibilityTimeout=FIFTEEN_MINUTES,
                )
                time.sleep(FIFTEEN_MINUTES)
                continue
            except twikit.errors.BadRequest:
                print("Followers: Bad Request")
                break
            except twikit.errors.NotFound:
                print("Followers: Not Found")
                break
            except twikit.errors.TwitterException as e:
                print(f"Followers: Twitter Exception {e}")
                break
            if num_iter % 5 == 0:
                print(f"Processed {num_iter} follower batches, sleeping...")
                time.sleep(1)

        return list(followers_dict.values())

    def x_get_followers(self, follower_count):
        """
        Pull up to 1000 followers via v1.1, convert each to a dict
        shape that parse_x_users expects, then parse.
        """
        api_v1_client = api_v1_creator()
        legacy_users = tweepy.Cursor(
            api_v1_client.get_followers,
            user_id=self.user_id,
            count=follower_count,
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

    def send_to_queue(self, user_id, queue_name):
        """
        Sends twikit or X user to the corresponding queue

        Args:
            - user_id (str)
            - queue_name (str)
        """
        queue_url = self.sqs_client.get_queue_url(QueueName=queue_name)[
            "QueueUrl"
        ]

        message = {
            "user_id": user_id,
            "location": self.location,
        }
        try:
            self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message),
            )
        except Exception as err:
            print(f"Unable to send user {user_id} to  {queue_name} SQS: {err}")

    def process_and_dispatch_followers(self, followers_list):
        """
        Process followers and save it to Neptune/SQS accordingly.

        Args:
        ----------
            - followers_list (list): List of user dicts
        """
        # Start Neptune client
        self.neptune_handler.start()

        existing_users_counter = 0
        root_users_counter = 0

        for follower_dict in followers_list:
            if self.neptune_handler.user_exists(follower_dict["user_id"]):
                existing_users_counter += 1
            else:
                self.neptune_handler.create_user_node(follower_dict)
                if (
                    self.further_extraction
                    and (
                        follower_dict["city"]
                        == follower_dict["target_location"]
                    )
                    and (
                        follower_dict["followers_count"]
                        > INFLUENCER_FOLLOWERS_THRESHOLD
                    )
                ):
                    root_users_counter += 1
                    self.send_to_queue(
                        follower_dict["user_id"], SQS_USER_TWEETS
                    )
                    self.send_to_queue(
                        follower_dict["user_id"], SQS_USER_FOLLOWERS
                    )
                    props_dict = {
                        "follower_status": "queued",
                        "last_updated": datetime.now(timezone.utc).isoformat(),
                    }
                    self.neptune_handler.update_node_attributes(
                        label="User",
                        node_id=follower_dict["user_id"],
                        props_dict=props_dict,
                    )

            self.neptune_handler.create_follower_edge(
                follower_dict["user_id"], self.user_id
            )

        props_dict = {}
        props_dict["follower_status"] = "completed"
        props_dict["follower_last_processed"] = datetime.now(
            timezone.utc
        ).isoformat()
        props_dict["last_updated"] = props_dict["follower_last_processed"]
        self.neptune_handler.update_node_attributes(
            label="User",
            node_id=self.user_id,
            props_dict=props_dict,
        )

        # Stop Neptune client
        self.neptune_handler.stop()

        print(
            f"### Root users identified: {root_users_counter}, Existing users: {existing_users_counter} ###"
        )


if __name__ == "__main__":
    parser = ArgumentParser(
        "Parameters to get followers data to generate a network"
    )
    parser.add_argument(
        "--num_followers", type=int, help="Number of followers to get"
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
        action="store_true",
        help="Enable further extraction of retweeters and followers",
    )

    print("Parsing arguments...")
    print()
    args = parser.parse_args()

    sqs_client = boto3.client("sqs", region_name="us-west-1")
    user_followers_queue_url = sqs_client.get_queue_url(
        QueueName=SQS_USER_FOLLOWERS
    )["QueueUrl"]
    neptune_handler = NeptuneHandler(NEPTUNE_ENDPOINT)

    user_counter = 0

    while True:
        # Pass Queue Name and get its URL
        response = sqs_client.receive_message(
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
        root_user_id = str(clean_data["user_id"])
        location = clean_data["location"]
        user_counter += 1

        print()
        print(
            f"Beginning followers extraction for User {user_counter} with ID {root_user_id}"
        )

        user_followers = UserFollowers(
            user_id=root_user_id,
            location=location,
            further_extraction=args.further_extraction,
            sqs_client=sqs_client,
            receipt_handle=receipt_handle,
            neptune_handler=neptune_handler,
        )

        if args.extraction_type == "twikit":
            print("Initiating twikit extraction...")
            followers_list = asyncio.run(
                user_followers.twikit_get_followers(
                    follower_count=args.num_followers,
                    account_num=args.account_num,
                )
            )
        elif args.extraction_type == "X":
            raise Exception(
                "X API Followers endpoint is only supported for Enterprise"
            )
            # followers_list = user_followers.x_get_followers(
            #     follower_count=args.num_followers
            # )

        print(f"### Total Followers extracted: {len(followers_list)} ###")

        if len(followers_list) == 0:
            print("Follower extraction FAILED. Moving on to the next user.\n")
            props_dict = {
                "follower_status": "failed",
                "follower_last_processed": datetime.now(
                    timezone.utc
                ).isoformat(),
            }
            props_dict["last_updated"] = props_dict["follower_last_processed"]
            neptune_handler.start()
            neptune_handler.update_node_attributes(
                label="User",
                node_id=root_user_id,
                props_dict=props_dict,
            )
            neptune_handler.stop()
            continue

        print("Processing and dispatching followers...")
        user_followers.process_and_dispatch_followers(followers_list)

        # Delete root user message from queue so it is not picked up again
        print("Deleting user message from queue")
        sqs_client.delete_message(
            QueueUrl=user_followers_queue_url,
            ReceiptHandle=receipt_handle,
        )
