"""
Script to pull tweets and retweeters from a particular user,
"""
import time

import twikit
from config_utils.constants import (
    ONE_MINUTE,
    TWIKIT_COOKIES_DIR,
    TWIKIT_FOLLOWERS_THRESHOLD,
    TWIKIT_RETWEETERS_THRESHOLD,
    TWIKIT_THRESHOLD,
)
from config_utils.util import network_json_maker


class UserNetwork:
    TWIKIT_THRESHOLD = TWIKIT_THRESHOLD
    TWIKIT_FOLLOWERS_THRESHOLD = TWIKIT_FOLLOWERS_THRESHOLD
    TWIKIT_RETWEETERS_THRESHOLD = TWIKIT_RETWEETERS_THRESHOLD
    TWIKIT_COOKIES_DIR = TWIKIT_COOKIES_DIR
    SLEEP_TIME = 2

    def __init__(self, output_file_path):
        self.client = twikit.Client("en-US")
        self.client.load_cookies(self.TWIKIT_COOKIES_DIR)
        self.output_file_path = output_file_path

        # Setting retweeters counter at the top level
        self.retweeters_counter = 0

    @staticmethod
    def parse_users(users):
        """
        Parse retweeters (user objects) and
        put them into a list of dictionaries
        """
        users_list = []

        if users:
            for user in users:
                user_dict = {}
                user_dict["user_id"] = user.id
                user_dict["username"] = user.screen_name
                user_dict["description"] = user.description
                user_dict["location"] = user.location
                user_dict["followers_count"] = user.followers_count
                user_dict["following_count"] = user.following_count

                users_list.append(user_dict)

        return users_list

    @staticmethod
    def parse_tweets(tweets):
        """
        Given a set of tweets, we get a list of dictionaries
        with the twees' and retweeters' information

        Args:
            - tweets: list of twikit.Tweet objects
        """
        dict_list = []
        if tweets:
            for tweet in tweets:
                tweet_dict = {}
                tweet_dict["tweet_id"] = tweet.id
                tweet_dict["tweet_text"] = tweet.text
                tweet_dict["created_at"] = tweet.created_at
                tweet_dict["retweet_count"] = tweet.retweet_count
                tweet_dict["favorite_count"] = tweet.favorite_count
                dict_list.append(tweet_dict)

        return dict_list

    async def get_single_tweet_retweeters(self, tweet_id):
        """
        For a particular tweet, get all the possible retweeters

        Args:
        ---------
            - tweet_id (str): String with tweet id
        """
        retweeters_list = []
        more_retweeters_available = True
        self.retweeters_counter += 1

        # Maxed out retweeters threshold
        if self.retweeters_counter == self.TWIKIT_RETWEETERS_THRESHOLD:
            print("Maxed out on retweeters threshold")
            self.retweeters_maxed_out = True
            return []

        try:
            retweeters = await self.client.get_retweeters(tweet_id)
            if retweeters:
                parsed_retweeters = self.parse_users(retweeters)
                retweeters_list.extend(parsed_retweeters)
        except twikit.errors.TooManyRequests:
            print("Retweeters: Too Many Requests")
            self.retweeters_maxed_out = True
            return None

        while more_retweeters_available:
            self.retweeters_counter += 1
            if self.retweeters_counter < self.TWIKIT_RETWEETERS_THRESHOLD:
                try:
                    more_retweeters = await retweeters.next()
                # Stop here if failure and return what you had so far
                except twikit.errors.TooManyRequests:
                    print("Retweeters: Too Many Requests")
                    print(f"Made {self.retweeters_counter} retweets requests")
                    self.retweeters_maxed_out = True
                    return retweeters_list
                except twikit.error.BadRequest:
                    print("Retweeters: Bad Request")
                    return retweeters_list
                if more_retweeters:
                    more_parsed_retweeters = self.parse_users(more_retweeters)
                    retweeters_list.extend(more_parsed_retweeters)
                else:
                    more_retweeters_available = False
            else:
                print("Maxed out on retweeters threshold")
                print(f"Made {self.retweeters_counter} retweets requests")
                self.retweeters_maxed_out = True
                return retweeters_list

        return retweeters_list

    async def add_retweeters(self, tweets_list):
        """
        For every tweet in a list of dictionaries, attempt to
        get all possible retweeters.

        Args
        --------
            tweets_list(list): List of dictionaries
        """
        new_tweets_list = []
        counter = 1
        # Variable to determine if no more requests on the retweeters
        self.retweeters_maxed_out = False

        for tweet_dict in tweets_list:
            # Only get retweeters if retweet_count > 0
            if tweet_dict["retweet_count"] > 0:
                if not self.retweeters_maxed_out:
                    retweeters = await self.get_single_tweet_retweeters(
                        tweet_dict["tweet_id"]
                    )
                    # If retweeters, we add that field to the dict
                    if isinstance(retweeters, list):
                        tweet_dict["retweeters"] = retweeters
            if counter % 200 == 0:
                print(f"Processed {counter} tweets")
            counter += 1
            new_tweets_list.append(tweet_dict)

        return new_tweets_list

    async def get_user_tweets(self, user_id):
        """
        For a given user, we get as many of their tweets as possible
        and parse them into a list

        Args
        -------
            - client: Twikit client obj

        Returns:
            - list of dictionaries
        """
        # We need to get tweets first
        dict_list = []
        more_tweets_available = True
        num_iter = 0

        # Parse first set of tweets
        user_tweets = await self.client.get_user_tweets(user_id, "Tweets")
        tweets_list = self.parse_tweets(user_tweets)
        dict_list.extend(tweets_list)

        while more_tweets_available:
            num_iter += 1
            try:
                next_tweets = await user_tweets.next()
                if next_tweets:
                    # Parse next tweets
                    tweets_list = self.parse_tweets(next_tweets)
                    dict_list.extend(tweets_list)
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

    async def get_followers(self, user_id):
        """
        Gets a given user's followers
        """
        followers_list = []
        followers = await self.client.get_user_followers(user_id)
        more_followers_available = True
        num_iter = 0

        parsed_followers = self.parse_users(followers)
        followers_list.extend(parsed_followers)

        while more_followers_available:
            num_iter += 1
            try:
                more_followers = await followers.next()
                if more_followers:
                    more_parsed_followers = self.parse_users(more_followers)
                    followers_list.extend(more_parsed_followers)
                else:
                    more_followers_available = False
            # Stop here and just return what you got
            except twikit.errors.TooManyRequests:
                print("Followers: too many requests, stopping...")
                return followers_list
            if num_iter % 5 == 0:
                print(f"Processed {num_iter} follower batches, sleeping...")
                time.sleep(self.SLEEP_TIME)
            if num_iter == self.TWIKIT_FOLLOWERS_THRESHOLD:
                print("Followers: maxed out number of requests")
                return followers_list

        return followers_list

    async def run(self, user_id):
        """
        Runs the pertinent functions by getting a user's retweeters and
        followers
        """
        user_dict = {}
        user_dict["user_id"] = user_id

        # First get tweets, without retweeters
        print("Getting user tweets")
        user_tweets = await self.get_user_tweets(user_id)
        time.sleep(ONE_MINUTE)
        print("Getting user retweeters")
        user_tweets = await self.add_retweeters(user_tweets)
        user_dict["tweets"] = user_tweets

        time.sleep(ONE_MINUTE)
        print("Getting user followers...")
        followers = await self.get_followers(user_id)
        user_dict["followers"] = followers

        network_json_maker(self.output_file_path, user_dict)
        print(f"Stored {user_id} data")


