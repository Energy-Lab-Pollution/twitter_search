"""
Script to get a given user's tweets
"""

import datetime
import json
import time
from argparse import ArgumentParser

import boto3
import twikit
from config_utils.constants import (
    FIFTEEN_MINUTES,
    TWIKIT_COOKIES_DICT,
    USER_FIELDS,
)
from config_utils.util import (
    client_creator,
    convert_to_iso_format,
)


class UserTweets:
    def __init__(self, location):
        self.location = location
        self.sqs_client = boto3.client("sqs", region='us-west-1')

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

        try:
            user_tweets = await self.client.get_user_tweets(
                user_id, "Tweets", count=num_tweets
            )
            num_extracted_tweets += len(tweets_list)
        except twikit.errors.TooManyRequests:
            print("User Tweets: Too Many Requests...")
            time.sleep(FIFTEEN_MINUTES)
            user_tweets = await self.client.get_user_tweets(
                user_id, "Tweets", count=num_tweets
            )
            num_extracted_tweets += len(tweets_list)
    
        tweets_list = self.parse_twikit_tweets(user_tweets)
        parsed_tweets_list.extend(tweets_list)

        while num_extracted_tweets < num_tweets:
            num_iter += 1
            try:
                if num_iter == 1:
                    next_tweets = await user_tweets.next()
                else:
                    next_tweets = await next_tweets.next()
                if next_tweets:
                    # Parse next tweets
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
        while len(user_tweets) < num_tweets:
            response = self.x_client.get_users_tweets(
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
