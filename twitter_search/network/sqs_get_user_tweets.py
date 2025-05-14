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
    EXPANSIONS,
    MAX_RESULTS,
    TWEET_FIELDS,
    TWIKIT_COOKIES_DIR,
    TWIKIT_TWEETS_THRESHOLD,
    USER_FIELDS,
)
from config_utils.util import (
    check_location,
    client_creator,
    convert_to_iso_format,
)


class UserTweets:
    def __init__(self, location, tweet_count):
        self.location = location
        self.sqs_client = boto3.client("sqs")
        self.tweet_count = tweet_count

    async def twikit_get_user_tweets(self, user_id):
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
        dict_list = []
        more_tweets_available = True
        num_iter = 0

        # Parse first set of tweets
        user_tweets = await self.client.get_user_tweets(
            user_id, "Tweets", count=self.TWIKIT_COUNT
        )
        tweets_list = self.parse_twikit_tweets(user_tweets)
        dict_list.extend(tweets_list)

        while more_tweets_available:
            num_iter += 1
            try:
                if num_iter == 1:
                    next_tweets = await user_tweets.next()
                else:
                    next_tweets = await next_tweets.next()
                if next_tweets:
                    # Parse next tweets
                    next_tweets_list = self.parse_twikit_tweets(next_tweets)
                    dict_list.extend(next_tweets_list)
                else:
                    more_tweets_available = False
            # If errored out on requests, just return what you already have
            except twikit.errors.TooManyRequests:
                print("Tweets: too many requests, stopping...")
                return dict_list
            if num_iter % 5 == 0:
                print(f"Processed {num_iter} user tweets batches")
                time.sleep(self.SLEEP_TIME)

            if num_iter >= self.TWIKIT_THRESHOLD:
                print("Tweets: Maxed out on requests")
                return dict_list

        return dict_list

    def x_get_user_tweets(self, user_id):
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
        while len(user_tweets) < X_MAX_USER_TWEETS:
            response = self.x_client.get_users_tweets(
                id=user_id,
                max_results=X_TWEETS_PAGE_SIZE,
                pagination_token=next_token,
                tweet_fields=["created_at", "public_metrics"],
            )

            page = response.date or []
            if not page:
                break

            user_tweets.extend(page)

            if (len(user_tweets) >= X_MAX_USER_TWEETS) or not (
                response.meta.get("next_token")
            ):
                break

        parsed_tweets = self.parse_x_tweets(user_tweets)

        return parsed_tweets
