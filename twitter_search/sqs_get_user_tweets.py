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
    NEPTUNE_S3_BUCKET,
    REGION_NAME,
    SQS_USER_RETWEETERS,
    SQS_USER_TWEETS,
    TWIKIT_COOKIES_DICT,
)
from config_utils.util import (
    client_creator,
    convert_to_iso_format,
)


SQS_CLIENT = boto3.client("sqs", region_name=REGION_NAME)


class UserTweets:
    def __init__(self, location, queue_url, receipt_handle):
        self.location = location
        self.queue_url = queue_url
        self.receipt_handle = receipt_handle

    @staticmethod
    def parse_twikit_tweets(tweets):
        """
        Given a set of tweets, we get a list of dictionaries
        with the tweets' and retweeters' information

        Args:
        ----------
            - tweets (list): list of twikit.Tweet objects
        Returns:
        ----------
            - dict_list (list): list of dictionaries with tweets info
        """
        dict_list = []
        if tweets:
            for tweet in tweets:
                tweet_dict = {}
                tweet_dict["tweet_id"] = tweet.id
                tweet_dict["tweet_text"] = tweet.text
                tweet_dict["created_at"] = convert_to_iso_format(
                    tweet.created_at
                )
                tweet_dict["retweet_count"] = tweet.retweet_count
                tweet_dict["favorite_count"] = tweet.favorite_count
                dict_list.append(tweet_dict)

        return dict_list

    @staticmethod
    def parse_x_tweets(tweets_list):
        """
        Normalize Tweepy v2 Tweet objects into dicts like your Twikit parser.
        """
        parsed_tweets = []
        for tweet in tweets_list:
            # t.created_at is a datetime
            created = (
                tweet.created_at.isoformat()
                if isinstance(tweet.created_at, datetime.datetime)
                else tweet.created_at
            )

            parsed_tweets.append(
                {
                    "tweet_id": tweet.id,
                    "tweet_text": tweet.text,
                    "created_at": created,
                    "retweet_count": tweet.public_metrics.get(
                        "retweet_count", 0
                    ),
                    "favorite_count": tweet.public_metrics.get("like_count", 0),
                }
            )

        return parsed_tweets

    def filter_tweets(self, tweets_list):
        """
        Keeps tweets that:
        - are unique
        - are original tweets (not retweets)

        Args:
            - tweets_list (list)
        Returns:
            - new_tweets_list (list)
            - s3_tweets (list)
            - last_tweeted_at (timestamp)
        """
        new_tweets_list = []
        s3_tweets = []
        unique_ids = []
        timestamps = []

        for tweet_dict in tweets_list:
            tweet_id = tweet_dict["tweet_id"]
            timestamp = datetime.datetime.fromisoformat(
                tweet_dict["created_at"]
            )
            timestamps.append(timestamp)
            if not tweet_dict["tweet_text"].startswith("RT @"):
                s3_tweets.append(tweet_dict)
                if tweet_dict["retweet_count"] > 0:
                    if tweet_id not in unique_ids:
                        unique_ids.append(str(tweet_id))
                        new_tweets_list.append(tweet_dict)
            else:
                continue

        last_tweeted_at = max(timestamps).isoformat() if timestamps else "null"

        return new_tweets_list, s3_tweets, last_tweeted_at

    async def twikit_get_user_tweets(self, user_id, num_tweets, account_num):
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
        parsed_tweets_list = []
        num_iter = 0
        num_extracted_tweets = 0

        # Parse first set of tweets

        # TODO: Filter inside this function
        try:
            user_tweets = await client.get_user_tweets(
                user_id, "Tweets", count=num_tweets
            )
            num_extracted_tweets += len(user_tweets)
        except twikit.errors.TooManyRequests:
            print("User Tweets: Too Many Requests...")
            time.sleep(FIFTEEN_MINUTES)
            user_tweets = await client.get_user_tweets(
                user_id, "Tweets", count=num_tweets
            )

        # Parsing and filtering tweets
        tweets_list = self.parse_twikit_tweets(user_tweets)
        num_extracted_tweets += len(tweets_list)
        parsed_tweets_list.extend(tweets_list)

        while num_extracted_tweets < num_tweets:
            num_iter += 1
            try:
                if num_iter == 1:
                    next_tweets = await user_tweets.next()
                else:
                    next_tweets = await next_tweets.next()
                if next_tweets:
                    # Parse next tweets and filter them as well
                    next_tweets_list = self.parse_twikit_tweets(next_tweets)
                    parsed_tweets_list.extend(next_tweets_list)
                    num_extracted_tweets += len(next_tweets_list)
                else:
                    print("No more tweets, moving on to next query")
                    return parsed_tweets_list
            # If errored out on requests, just return what you already have
            except twikit.errors.TooManyRequests:
                print("Tweets: too many requests, stopping...")
                time.sleep(FIFTEEN_MINUTES)
                SQS_CLIENT.change_message_visibility(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=self.receipt_handle,
                    VisibilityTimeout=FIFTEEN_MINUTES,
                )
            if num_iter % 5 == 0:
                print(f"Processed {num_iter} user tweets batches")

        parsed_tweets_list, s3_tweets, last_tweeted_at = self.filter_tweets(
            parsed_tweets_list
        )
        return parsed_tweets_list, s3_tweets, last_tweeted_at

    def x_get_user_tweets(self, user_id, num_tweets):
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
                id=user_id,
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

        parsed_tweets = self.parse_x_tweets(user_tweets)
        tweets_list, s3_tweets, last_tweeted_at = self.filter_tweets(
            parsed_tweets
        )

        return tweets_list, s3_tweets, last_tweeted_at

    def insert_tweets_to_s3(self, user_id, tweets_list):
        """
        Function to insert each user's tweets as
        a txt files to S3

        These tweets are already filtered
        """
        s3_client = boto3.client("s3", region_name=REGION_NAME)
        for tweet in tweets_list:
            s3_path = f"networks/{self.location}/classification/{user_id}/input/tweet_{tweet['tweet_id']}.txt"
            try:
                s3_client.put_object(
                    Bucket=NEPTUNE_S3_BUCKET,
                    Key=s3_path,
                    Body=tweet["tweet_text"].encode("utf-8", errors="ignore"),
                )
            except botocore.exceptions.ClientError:
                print(f"Unable to upload {tweet['tweet_id']} for {user_id}")
                continue

    def send_to_queue(self, tweets_list, user_id, queue_name):
        """
        Sends tweet objects to the corresponding queue

        Args:
            - tweets_list (list): List with the root user's tweet objects
            - user_id (str): String with the root user's id
            - queue_name (str): Queue Name
        """
        queue_url = SQS_CLIENT.get_queue_url(QueueName=queue_name)["QueueUrl"]
        if not tweets_list:
            print("No tweets to send to queue")
            return
        for tweet in tweets_list:
            print(f"Sending tweet {tweet['tweet_id']}")
            # TODO: Modify group
            message = {
                "tweet_id": tweet["tweet_id"],
                "target_user_id": user_id,
                "location": self.location,
            }
            try:
                SQS_CLIENT.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(message),
                    MessageGroupId=user_id,
                )
                print(
                    f"Tweet {tweet['tweet_id']} for {user_id} sent to {queue_name} queue :)"
                )
            except Exception as err:
                print(
                    f"Unable to send tweet {tweet['tweet_id']} for {user_id} to {queue_name} SQS: {err}"
                )
                continue


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

    args = parser.parse_args()
    user_tweets_queue_url = SQS_CLIENT.get_queue_url(QueueName=SQS_USER_TWEETS)[
        "QueueUrl"
    ]

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
            clean_data = json.loads(message["Body"])

        except KeyError:
            # Empty queue
            print("Empty queue")
            continue

        # Getting information from body message
        print(clean_data)
        root_user_id = str(clean_data["user_id"])
        location = clean_data["location"]

        user_tweets = UserTweets(
            location, user_tweets_queue_url, receipt_handle
        )

        if args.extraction_type == "twikit":
            tweets_list, s3_tweets, last_tweeted_at = asyncio.run(
                user_tweets.twikit_get_user_tweets(
                    user_id=root_user_id,
                    num_tweets=args.tweet_count,
                    account_num=args.account_num,
                )
            )
        elif args.extraction_type == "X":
            tweets_list, s3_tweets, last_tweeted_at = (
                user_tweets.x_get_user_tweets(
                    user_id=root_user_id, num_tweets=args.tweet_count
                )
            )

        print(f"Got {len(tweets_list)} with retweets")
        print(f"Last tweeted at {last_tweeted_at}")
        print(f"Uploading {len(s3_tweets)} tweets to S3...")
        user_tweets.insert_tweets_to_s3(root_user_id, s3_tweets)

        # TODO: Insert last_tweeted_at field to neptune

        # Send tweets to retweeters queue
        user_tweets.send_to_queue(
            tweets_list, user_id=root_user_id, queue_name=SQS_USER_RETWEETERS
        )

        # TODO: "retweeter_status": pending, queued, in_progress, "completed", "failed
        # here, retweeter_status will be set as queued

        # Delete root user message from queue so it is not picked up again
        SQS_CLIENT.delete_message(
            QueueUrl=user_tweets_queue_url,
            ReceiptHandle=receipt_handle,
        )
