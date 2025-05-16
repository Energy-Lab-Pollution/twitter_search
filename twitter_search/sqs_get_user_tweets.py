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


SQS_CLIENT = boto3.client("sqs", region="us-west-1")


class UserTweets:
    def __init__(self, location):
        self.location = location

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
                if isinstance(tweet.created_at, datetime)
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

    @staticmethod
    def filter_tweets(tweets_list):
        """
        Keeps tweets that:
        - are unique
        - are original tweets (not retweets)

        Args:
            - tweets_list (list)
        Returns:
            - new_tweets_list (list)
        """
        new_tweets_list = []
        unique_ids = []

        for tweet_dict in tweets_list:
            tweet_id = tweet_dict["tweet_id"]
            if (not tweet_dict["tweet_text"].startswith("RT @")) and (
                tweet_dict["retweet_count"] > 0
            ):
                if tweet_id not in unique_ids:
                    unique_ids.append(str(tweet_id))
                    new_tweets_list.append(new_tweets_list)
            else:
                continue

        return new_tweets_list

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
        client.load_cookies(TWIKIT_COOKIES_DICT[f"account_{account_num}"])
        parsed_tweets_list = []
        num_iter = 0
        num_extracted_tweets = 0

        # Parse first set of tweets

        # TODO: Get user tweets
        try:
            user_tweets = await self.client.get_user_tweets(
                user_id, "Tweets", count=num_tweets
            )
            num_extracted_tweets += len(user_tweets)
        except twikit.errors.TooManyRequests:
            print("User Tweets: Too Many Requests...")
            time.sleep(FIFTEEN_MINUTES)
            user_tweets = await self.client.get_user_tweets(
                user_id, "Tweets", count=num_tweets
            )

        # Parsing and filtering tweets
        tweets_list = self.parse_twikit_tweets(user_tweets)
        tweets_list = self.filter_tweets(tweets_list)
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
                    next_tweets_list = self.filter_tweets(next_tweets_list)
                    parsed_tweets_list.extend(next_tweets_list)
                    num_extracted_tweets += len(next_tweets_list)
                else:
                    print("No more tweets, moving on to next query")
                    return parsed_tweets_list
            # If errored out on requests, just return what you already have
            except twikit.errors.TooManyRequests:
                print("Tweets: too many requests, stopping...")
                time.sleep(FIFTEEN_MINUTES)
            if num_iter % 5 == 0:
                print(f"Processed {num_iter} user tweets batches")

        return parsed_tweets_list

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

            page = response.date or []
            if not page:
                break

            user_tweets.extend(page)

            if (len(user_tweets) >= num_tweets) or not (
                response.meta.get("next_token")
            ):
                break

        parsed_tweets = self.parse_x_tweets(user_tweets)

        return parsed_tweets

    def insert_tweets_to_s3(self, user_id, tweets_list):
        """
        Function to insert each user's tweets as
        a txt files to S3
        """
        s3_client = boto3.client("s3", region_name=REGION_NAME)
        for tweet in tweets_list:
            s3_path = f"networks/{self.location}/classification/{user_id}/input/tweet_{tweet['tweet_id']}.txt"
            try:
                s3_client.put_object(
                    Bucket=NEPTUNE_S3_BUCKET,
                    Key=s3_path,
                    Body=tweet['tweet_text'].encode('utf-8', errors='ignore')
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
            message = {
                "tweet_id": tweet["tweet_id"],
                "target_user_id": user_id,
                "location": self.location,
            }
            try:
                SQS_CLIENT.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(message),
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
    user_tweets_queue_url = SQS_CLIENT.get_queue_url(QueueName=SQS_USER_TWEETS)["QueueUrl"]

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

        root_user_id = str(clean_data["user_id"])
        location = clean_data["location"]

        user_tweets = UserTweets(location)

        if args.extraction_type == "twikit":
            tweets_list = asyncio.run(
                user_tweets.twikit_get_user_tweets(
                    user_id=root_user_id,
                    num_tweets=args.tweet_count,
                    account_num=args.account_num,
                )
            )
        elif args.extraction_type == "X":
            tweets_list = user_tweets.x_get_user_tweets(
                user_id=root_user_id, num_tweets=args.tweet_count
            )

        # TODO: Dump tweets_list to S3

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
