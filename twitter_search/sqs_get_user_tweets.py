"""
Script to get a given user's tweets from a Queue,
the tweets are sent to a separate SQS queue to get retweeters

Author: Federico Dominguez Molina & Vishal Joseph
Last Updated: May 2025
"""

import asyncio
import datetime
import json
import time
from argparse import ArgumentParser
from pathlib import Path

import boto3
import botocore
import twikit
from config_utils.constants import (
    FIFTEEN_MINUTES,
    NEPTUNE_ENDPOINT,
    NEPTUNE_S3_BUCKET,
    SQS_USER_RETWEETERS,
    SQS_USER_TWEETS,
    TWENTYFIVE_MINUTES,
    TWIKIT_COOKIES_DICT,
)
from config_utils.neptune_handler import NeptuneHandler
from config_utils.util import (
    client_creator,
    convert_to_iso_format,
)


class UserTweets:
    def __init__(
        self, user_id, location, sqs_client, receipt_handle, neptune_handler
    ):
        self.user_id = user_id
        self.location = location
        self.sqs_client = sqs_client
        self.s3_client = boto3.client("s3", region_name="us-east-2")
        self.receipt_handle = receipt_handle
        self.neptune_handler = neptune_handler

    @staticmethod
    def parse_twikit_tweets(tweets):
        """
        Given a set of tweets, we get a dict of dictionaries
        with the tweets' and retweeters' information

        Args:
        ----------
            - tweets (list): list of twikit.Tweet objects
        Returns:
        ----------
            - tweets_dict (dict): dict of dictionaries with tweets info
        """
        tweets_dict = {}
        if tweets:
            for tweet in tweets:
                if tweet.id in tweets_dict:
                    continue
                tweet_dict = {}
                tweet_dict["tweet_id"] = tweet.id
                tweet_dict["tweet_text"] = tweet.text
                tweet_dict["created_at"] = convert_to_iso_format(
                    tweet.created_at
                )
                tweet_dict["retweet_count"] = tweet.retweet_count
                tweet_dict["favorite_count"] = tweet.favorite_count
                tweets_dict[tweet.id] = tweet_dict

        return tweets_dict

    @staticmethod
    def parse_x_tweets(tweets_list):
        """
        Normalize Tweepy v2 Tweet objects into dicts like your Twikit parser.
        """
        parsed_tweets_dict = {}
        for tweet in tweets_list:
            if tweet.id in parsed_tweets_dict:
                continue
            # t.created_at is a datetime
            created = (
                tweet.created_at.isoformat()
                if isinstance(tweet.created_at, datetime.datetime)
                else tweet.created_at
            )
            tweet_dict = {
                "tweet_id": tweet.id,
                "tweet_text": tweet.text,
                "created_at": created,
                "retweet_count": tweet.public_metrics.get("retweet_count", 0),
                "favorite_count": tweet.public_metrics.get("like_count", 0),
            }
            parsed_tweets_dict[tweet.id] = tweet_dict

        return parsed_tweets_dict

    async def twikit_get_user_tweets(self, num_tweets, account_num):
        """
        For a given user, we get as many of their tweets as possible
        and parse them into a list using twikit

        Args
        -------
            - client: Twikit client obj

        Returns:
        ---------
            - dict_list (list): list of dictionaries
        """
        # We need to get tweets first
        client = twikit.Client("en-US")
        cookies_dir = TWIKIT_COOKIES_DICT[f"account_{account_num}"]
        cookies_dir = Path(__file__).parent.parent / cookies_dir
        client.load_cookies(cookies_dir)
        queue_url = self.sqs_client.get_queue_url(QueueName=SQS_USER_TWEETS)[
            "QueueUrl"
        ]
        parsed_tweets_dict = {}
        num_iter = 0
        num_extracted_tweets = 0

        flag = False
        for _ in range(3):
            # Parse first set of tweets
            try:
                user_tweets = await client.get_user_tweets(
                    self.user_id, "Tweets", count=num_tweets
                )
                if not user_tweets:
                    return []
                flag = True
                # Parsing and filtering tweets
                tweets_dict = self.parse_twikit_tweets(user_tweets)
                num_extracted_tweets += len(tweets_dict)
                parsed_tweets_dict = parsed_tweets_dict | tweets_dict
                num_iter += 1
                break
            except twikit.errors.TooManyRequests:
                print("User Tweets: Too Many Requests...")
                self.sqs_client.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=self.receipt_handle,
                    VisibilityTimeout=TWENTYFIVE_MINUTES,
                )
                time.sleep(FIFTEEN_MINUTES)
                continue
            except Exception as e:
                print(f"Tweet extraction failed: {e}")
                continue

        if not flag:
            print("No tweets extracted despite 3 retry attempts")
            return []

        while num_extracted_tweets < num_tweets:
            try:
                if num_iter == 1:
                    next_tweets = await user_tweets.next()
                else:
                    next_tweets = await next_tweets.next()
                if next_tweets:
                    # Parse next tweets and filter them as well
                    next_tweets_dict = self.parse_twikit_tweets(next_tweets)
                    parsed_tweets_dict = parsed_tweets_dict | next_tweets_dict
                    num_extracted_tweets += len(next_tweets_dict)
                    num_iter += 1
                else:
                    print("No more tweets, moving on...")
                    break
            # If errored out on requests, just return what you already have
            except twikit.errors.TooManyRequests:
                print("Tweets: too many requests, stopping...")
                self.sqs_client.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=self.receipt_handle,
                    VisibilityTimeout=TWENTYFIVE_MINUTES,
                )
                time.sleep(FIFTEEN_MINUTES)
                continue

            if num_iter % 5 == 0:
                print(f"Processed {num_iter} user tweets batches")

        return list(parsed_tweets_dict.values())

    def x_get_user_tweets(self, num_tweets):
        """
        For a given user, we get as many of their tweets as possible
        and parse them into a list using x

        Args
        -------
            - client: Twikit client obj

        Returns:
        ---------
            - dict_list (list): list of dictionaries
        """
        user_tweets = []
        next_token = None
        x_client = client_creator()
        while len(user_tweets) < num_tweets:
            response = x_client.get_users_tweets(
                id=self.user_id,
                max_results=num_tweets,
                pagination_token=next_token,
                tweet_fields=["created_at", "public_metrics"],
            )

            page = response.data or []
            if not page:
                break

            user_tweets.extend(page)

            if (len(user_tweets) >= num_tweets) or not (
                response.meta.get("next_token")
            ):
                break

        parsed_tweets_dict = self.parse_x_tweets(user_tweets)

        return list(parsed_tweets_dict.values())

    def insert_tweet_to_s3(self, tweet):
        """
        Function to insert each user's tweets as
        a txt files to S3

        These tweets are already filtered
        """
        s3_path = f"networks/{self.location}/classification/{self.user_id}/input/tweet_{tweet['tweet_id']}.txt"
        try:
            self.s3_client.put_object(
                Bucket=NEPTUNE_S3_BUCKET,
                Key=s3_path,
                Body=tweet["tweet_text"].encode("utf-8", errors="ignore"),
            )
        except botocore.exceptions.ClientError:
            print(f"Unable to upload {tweet['tweet_id']} for {self.user_id}")

    def send_to_queue(self, tweet_id, queue_name):
        """
        Sends tweet objects to the corresponding queue

        Args:
            - tweet_id (str): The tweet id
            - queue_name (str): Queue Name
        """
        queue_url = self.sqs_client.get_queue_url(QueueName=queue_name)[
            "QueueUrl"
        ]
        # TODO: Modify group
        message = {
            "tweet_id": tweet_id,
            "target_user_id": self.user_id,
            "location": self.location,
        }
        try:
            self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message),
                MessageGroupId=self.user_id,
            )
        except Exception as err:
            print(
                f"Unable to send tweet {tweet_id} for {self.user_id} to {queue_name} SQS: {err}"
            )

    def process_and_dispatch_tweets(self, tweets_list):
        """
        Filter tweets and save it to S3/Neptune accordingly.

        Args:
        ----------
            - tweets_list (list): list of tweet dicts
        """
        # Start Neptune client
        self.neptune_handler.start()

        timestamps = []

        s3_counter = 0
        filtered_tweet_counter = 0

        for tweet_dict in tweets_list:
            timestamp = datetime.datetime.fromisoformat(
                tweet_dict["created_at"]
            )
            timestamps.append(timestamp)
            if not tweet_dict["tweet_text"].startswith("RT @"):
                self.insert_tweet_to_s3(tweet_dict)
                s3_counter += 1
                if tweet_dict["retweet_count"] > 0:
                    filtered_tweet_counter += 1
                    self.send_to_queue(
                        tweet_dict["tweet_id"], queue_name=SQS_USER_RETWEETERS
                    )

        last_tweeted_at = max(timestamps).isoformat() if timestamps else "null"

        # Updating user attributes
        props_dict = {}
        if filtered_tweet_counter > 0:
            props_dict["retweeter_status"] = "queued"
        else:
            props_dict["retweeter_status"] = "completed"
            props_dict["retweeter_last_processed"] = datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat()
        props_dict["last_tweeted_at"] = last_tweeted_at
        props_dict["last_updated"] = datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat()
        self.neptune_handler.update_node_attributes(
            label="User",
            node_id=self.user_id,
            props_dict=props_dict,
        )

        # Stop Neptune client
        self.neptune_handler.stop()

        print(
            f"### Original tweets: {s3_counter}, Tweets with retweets: {filtered_tweet_counter} ###"
        )


if __name__ == "__main__":
    parser = ArgumentParser(
        "Parameters to get users data to generate a network"
    )
    parser.add_argument(
        "--tweet_count", type=int, help="Number of tweets to get"
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

    print("Parsing arguments...")
    print()
    args = parser.parse_args()

    sqs_client = boto3.client("sqs", region_name="us-west-1")
    user_tweets_queue_url = sqs_client.get_queue_url(QueueName=SQS_USER_TWEETS)[
        "QueueUrl"
    ]
    neptune_handler = NeptuneHandler(NEPTUNE_ENDPOINT)

    user_counter = 0

    while True:
        # Pass Queue Name and get its URL
        response = sqs_client.receive_message(
            QueueUrl=user_tweets_queue_url,
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
            f"Beginning tweet extraction for User {user_counter} with ID {root_user_id}"
        )

        user_tweets = UserTweets(
            root_user_id, location, sqs_client, receipt_handle, neptune_handler
        )

        if args.extraction_type == "twikit":
            print("Initiating twikit extraction...")
            tweets_list = asyncio.run(
                user_tweets.twikit_get_user_tweets(
                    num_tweets=args.tweet_count,
                    account_num=args.account_num,
                )
            )
        elif args.extraction_type == "X":
            print("Initiating X API extraction...")
            tweets_list = user_tweets.x_get_user_tweets(
                num_tweets=args.tweet_count
            )

        print(f"### Total tweets extracted: {len(tweets_list)} ###")

        if len(tweets_list) == 0:
            print("Tweet extraction FAILED. Moving on to the next user.")
            continue

        print("Processing and dispatching tweets...")
        user_tweets.process_and_dispatch_tweets(tweets_list)

        # Delete root user message from queue so it is not picked up again
        print("Deleting user message from queue")
        sqs_client.delete_message(
            QueueUrl=user_tweets_queue_url,
            ReceiptHandle=receipt_handle,
        )
