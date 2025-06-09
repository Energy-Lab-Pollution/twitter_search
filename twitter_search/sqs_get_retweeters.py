"""
Script to get a determined tweet's retweeters
"""

import asyncio
import json
import time
from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path

import boto3
import twikit
from config_utils.constants import (
    FIFTEEN_MINUTES,
    INFLUENCER_FOLLOWERS_THRESHOLD,
    NEPTUNE_ENDPOINT,
    SQS_USER_FOLLOWERS,
    SQS_USER_RETWEETERS,
    SQS_USER_TWEETS,
    TWENTYFIVE_MINUTES,
    TWIKIT_COOKIES_DICT,
)
from config_utils.neptune_handler import NeptuneHandler
from config_utils.util import (
    check_location,
    client_creator,
    convert_to_iso_format,
)


class UserRetweeters:
    def __init__(
        self, user_id, location, further_extraction, sqs_client, neptune_handler
    ):
        self.user_id = user_id
        self.location = location
        self.further_extraction = further_extraction
        self.sqs_client = sqs_client
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
            user_dict["city"] = self.location if location_match else "null"
            user_dicts.append(user_dict)

        return user_dicts

    def parse_twikit_users(self, users):
        """
        Parse retweeters (user objects) and put them
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
                user_dict["city"] = self.location if location_match else "null"
                users_dict[user.id] = user_dict

        return users_dict

    async def twikit_get_single_tweet_retweeters(
        self, tweet_id, num_retweeters, account_num, receipt_handle
    ):
        """
        For a particular tweet, get all the possible retweeters

        Args:
        ---------
            - tweet_id (str): String with tweet id
            - num_retweeters (int)
            - account_num (int)
            - receipt_handle (str)
        Returns:
        ---------
            - retweeters_list (list): List with retweeters info
        """
        client = twikit.Client("en-US")
        cookies_dir = TWIKIT_COOKIES_DICT[f"account_{account_num}"]
        cookies_dir = Path(__file__).parent.parent / cookies_dir
        client.load_cookies(cookies_dir)
        queue_url = self.sqs_client.get_queue_url(
            QueueName=SQS_USER_RETWEETERS
        )["QueueUrl"]
        retweeters_dict = {}
        extracted_retweeters = 0
        num_iter = 0

        flag = False
        for _ in range(3):
            try:
                retweeters = await client.get_retweeters(
                    tweet_id, count=num_retweeters
                )
                if not retweeters:
                    print("API call was successful but no output extracted")
                    return []
                flag = True
                parsed_retweeters = self.parse_twikit_users(retweeters)
                retweeters_dict = retweeters_dict | parsed_retweeters
                extracted_retweeters += len(parsed_retweeters)
                num_iter += 1
                break
            except twikit.errors.TooManyRequests:
                print("Retweeters: Too Many Requests")
                self.sqs_client.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle,
                    VisibilityTimeout=TWENTYFIVE_MINUTES,
                )
                time.sleep(FIFTEEN_MINUTES)
                continue
            except Exception as e:
                print(f"Retweeter extraction failed: {e}")
                continue

        if not flag:
            print("No retweeters extracted despite 3 retry attempts")
            return []

        while extracted_retweeters < num_retweeters:
            try:
                if num_iter == 1:
                    more_retweeters = await retweeters.next()
                else:
                    more_retweeters = await more_retweeters.next()
                if more_retweeters:
                    more_parsed_retweeters = self.parse_twikit_users(
                        more_retweeters
                    )
                    retweeters_dict = retweeters_dict | more_parsed_retweeters
                    extracted_retweeters += len(more_parsed_retweeters)
                    num_iter += 1
                else:
                    print("No more retweeters available")
                    break
            except twikit.errors.TooManyRequests:
                print("Retweeters: Too Many Requests")
                self.sqs_client.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle,
                    VisibilityTimeout=TWENTYFIVE_MINUTES,
                )
                time.sleep(FIFTEEN_MINUTES)
                continue
            except twikit.errors.BadRequest:
                print("Retweeters: Bad Request")
                break
            except twikit.errors.TwitterException as e:
                print(f"Retweeters: Twitter Exception {e}")
                break
            if num_iter % 5 == 0:
                print(f"Processed {num_iter} retweeters batches")

        return list(retweeters_dict.values())

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

    def process_and_dispatch_retweeters(self, tweet_id, user_retweeters_list):
        """
        Process retweeters and save it to Neptune/SQS accordingly.

        Args:
        ----------
            - tweet_id (str)
            - user_retweeters_list (list): List of user dicts
        """
        # Start Neptune client
        self.neptune_handler.start()

        existing_users_counter = 0
        root_users_counter = 0

        for retweeter_dict in user_retweeters_list:
            if self.neptune_handler.user_exists(retweeter_dict["user_id"]):
                existing_users_counter += 1
            else:
                self.neptune_handler.create_user_node(retweeter_dict)
                if (
                    self.further_extraction
                    and (
                        retweeter_dict["city"]
                        == retweeter_dict["target_location"]
                    )
                    and (
                        retweeter_dict["followers_count"]
                        > INFLUENCER_FOLLOWERS_THRESHOLD
                    )
                    and (retweeter_dict["tweets_count"] > 0)
                ):
                    root_users_counter += 1
                    self.send_to_queue(
                        retweeter_dict["user_id"], SQS_USER_TWEETS
                    )
                    self.send_to_queue(
                        retweeter_dict["user_id"], SQS_USER_FOLLOWERS
                    )
                    props_dict = {
                        "follower_status": "queued",
                        "last_updated": datetime.now(timezone.utc).isoformat(),
                    }
                    self.neptune_handler.update_node_attributes(
                        label="User",
                        node_id=retweeter_dict["user_id"],
                        props_dict=props_dict,
                    )

            self.neptune_handler.create_retweeter_edge(
                source_id=retweeter_dict["user_id"],
                target_id=self.user_id,
                tweet_id=tweet_id,
            )

        # Stop Neptune client
        self.neptune_handler.stop()

        print(
            f"Root users identified: {root_users_counter}, Existing users: {existing_users_counter}"
        )


if __name__ == "__main__":
    parser = ArgumentParser(
        "Parameters to get Retweeters data to generate a network"
    )
    parser.add_argument(
        "--num_retweeters", type=int, help="Number of retweeters to get"
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
    user_retweeters_queue_url = sqs_client.get_queue_url(
        QueueName=SQS_USER_RETWEETERS
    )["QueueUrl"]
    neptune_handler = NeptuneHandler(NEPTUNE_ENDPOINT)

    tmp_user_id = None
    user_counter = 0
    tweet_counter = 0

    while True:
        response = sqs_client.receive_message(
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
            if tmp_user_id is not None:
                print("----------------------------")
                print("Updating retweeter status and last processed")
                props_dict = {
                    "retweeter_status": "completed",
                    "retweeter_last_processed": datetime.now(
                        timezone.utc
                    ).isoformat(),
                }
                props_dict["last_updated"] = props_dict[
                    "retweeter_last_processed"
                ]
                neptune_handler.start()
                neptune_handler.update_node_attributes(
                    label="User",
                    node_id=tmp_user_id,
                    props_dict=props_dict,
                )
                neptune_handler.stop()
                print(f"### Total tweets processed: {tweet_counter} ###")
                print()
                tmp_user_id = None
                target_user_id = None
            print("Empty queue")
            continue

        # Getting information from body message
        tweet_id = str(clean_data["tweet_id"])
        target_user_id = str(clean_data["target_user_id"])
        location = clean_data["location"]

        # Check if target user has changed
        if tmp_user_id != target_user_id:
            neptune_handler.start()
            if tmp_user_id is not None:
                print("----------------------------")
                print("Updating retweeter status and last processed")
                props_dict = {
                    "retweeter_status": "completed",
                    "retweeter_last_processed": datetime.now(
                        timezone.utc
                    ).isoformat(),
                }
                props_dict["last_updated"] = props_dict[
                    "retweeter_last_processed"
                ]
                neptune_handler.update_node_attributes(
                    label="User",
                    node_id=tmp_user_id,
                    props_dict=props_dict,
                )
                print(f"### Total tweets processed: {tweet_counter} ###")
            retweeter_status = neptune_handler.extract_node_attribute(
                label="User",
                node_id=target_user_id,
                attribute_name="retweeter_status",
            )

            neptune_handler.stop()

            if not retweeter_status:
                raise ValueError("retweeter_status cannot return NULL value")

            if retweeter_status == "pending":
                print(
                    f"Target user {target_user_id} not ready for retweeter extraction"
                )
                continue

            user_counter += 1
            tweet_counter = 0

            print()
            print(
                f"Beginning retweeters extraction for User {user_counter} with ID {target_user_id}"
            )

            # Creating new class object
            user_retweeters = UserRetweeters(
                user_id=target_user_id,
                location=location,
                further_extraction=args.further_extraction,
                sqs_client=sqs_client,
                neptune_handler=neptune_handler,
            )

            # Re-aligning extraction focus on new target user
            tmp_user_id = target_user_id

        tweet_counter += 1
        print(f"----Tweet {tweet_counter}---")

        if args.extraction_type == "twikit":
            print("Initiating twikit extraction...")
            user_retweeters_list = asyncio.run(
                user_retweeters.twikit_get_single_tweet_retweeters(
                    tweet_id=tweet_id,
                    num_retweeters=args.num_retweeters,
                    account_num=args.account_num,
                    receipt_handle=receipt_handle,
                )
            )
        elif args.extraction_type == "X":
            print("Initiating X API extraction...")
            user_retweeters_list = (
                user_retweeters.x_get_single_tweet_retweeters(
                    tweet_id=tweet_id, num_retweeters=args.num_retweeters
                )
            )

        print(f"Retweeters extracted: {len(user_retweeters_list)}")

        if len(user_retweeters_list) == 0:
            print("Retweeter extraction FAILED. Moving on to the next tweet.")
            continue

        print("Processing and dispatching retweeters...")
        user_retweeters.process_and_dispatch_retweeters(
            tweet_id, user_retweeters_list
        )

        # Delete tweet message from queue so it is not picked up again
        print("Deleting tweet message from queue")
        sqs_client.delete_message(
            QueueUrl=user_retweeters_queue_url,
            ReceiptHandle=receipt_handle,
        )
