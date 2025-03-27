"""
Script to pull tweets and retweeters from a particular user,
"""
import os
import time

import twikit
from config_utils.util import json_maker
from config_utils.constants import (
    TWIKIT_COOKIES_DIR,
    TWIKIT_FOLLOWERS_THRESHOLD,
    TWIKIT_RETWEETERS_THRESHOLD,
    TWIKIT_THRESHOLD,
)


class UserNetwork:
    TWIKIT_THRESHOLD = TWIKIT_THRESHOLD
    TWIKIT_FOLLOWERS_THRESHOLD = TWIKIT_FOLLOWERS_THRESHOLD
    TWIKIT_RETWEETERS_THRESHOLD = TWIKIT_RETWEETERS_THRESHOLD
    TWIKIT_COOKIES_DIR = TWIKIT_COOKIES_DIR
    SLEEP_TIME = 10

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

    async def get_single_tweet_retweeters(self, tweet):
        """
        For a particular tweet, get all the possible retweeters

        Args:
        ---------
            - tweet: twikit.Tweet object
        """
        retweeters_list = []
        more_retweeters_available = True
        self.retweeters_counter += 1
        
        # Maxed out retweeters threshold
        if self.retweeters_counter == self.TWIKIT_RETWEETERS_THRESHOLD:
            print("Maxed out on retweeters threshold")
            return []

        try:                    
            retweeters = await tweet.get_retweeters()
            if retweeters:
                retweeters = self.parse_users(retweeters)
                retweeters_list.extend(retweeters)
        except twikit.errors.TooManyRequests:
            print("Retweeters: Too Many Requests")
            return None
        
        while more_retweeters_available:
            self.retweeters_counter += 1
            if self.retweeters_counter < self.TWIKIT_RETWEETERS_THRESHOLD:
                try:
                    more_retweeters = await retweeters.next()
                # Stop here if failure and return what you had so far
                except twikit.errors.TooManyRequests:
                    print("Retweeters: Too Many Requests")
                    return retweeters_list
                if more_retweeters:
                    more_retweeters = self.parse_users(more_retweeters)
                    retweeters_list.extend(retweeters_list)
                else:
                    more_retweeters_available = False
            else:
                print("Maxed out on retweeters threshold")
               
        return retweeters_list

    async def get_tweets_retweeters(self, tweets):
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
                retweeters = await self.get_single_tweet_retweeters(tweet)
                # If retweeters, we add that field to the dict
                if isinstance(retweeters, list):
                    retweeters = self.parse_users(retweeters)
                    tweet_dict["retweeters"] = retweeters
                dict_list.append(tweet_dict)

        return dict_list

    async def get_user_retweeters(self, user_id):
        """
        For a given user, we get as many of their tweets as possible.
        Then, for each tweet, we get the corresponding retweeters.

        Args
        -------
            - client: Twikit client obj

        Returns:
            - user_tweets:
        """
        # We need to get tweets first
        dict_list = []
        more_tweets_available = True
        num_iter = 0

        # Parse first set of tweets and get retweeters
        user_tweets = await self.client.get_user_tweets(user_id, "Tweets")
        tweets_list = self.get_tweets_retweeters(user_tweets)
        dict_list.extend(tweets_list)

        while more_tweets_available:
            num_iter += 1
            try:
                next_tweets = await user_tweets.next()
                if next_tweets:
                    # Parse next tweets and get retweeters
                    tweets_list = self.get_tweets_retweeters(next_tweets)
                    dict_list.extend(tweets_list)
                else:
                    more_tweets_available = False
            # If errored out on requests, stop there
            # Just return what you already have
            except twikit.errors.TooManyRequests:
                print("Followers: too many requests, stopping...")
                return dict_list               
            if num_iter % 5 == 0:
                print(f"Processed {num_iter} user tweets batches")

            if num_iter == self.TWIKIT_THRESHOLD:
                print("Followers: Maxed out on requests")
                return dict_list

            num_iter += 1

        return dict_list

    async def get_followers(self, user_id):
        """
        Gets a given user's followers
        """
        followers_list = []
        followers = await self.client.get_followers(user_id)
        more_followers_available = True
        num_iter = 0

        followers = self.parse_users(followers)
        followers_list.extend(followers)

        while more_followers_available:
            num_iter += 1
            try:
                more_followers = await followers.next()
                if more_followers:
                    more_followers = self.parse_users(more_followers)
                    followers_list.extend(more_followers)
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

    def store_user_data(self):
        """
        Stores user data in the provided path

        The util function checks for any existing dictionaries and
        only adds the newer data
        """
        json_maker(self.output_file_path, self.user_dict)

    async def run(self, user_id):
        """
        Runs the pertinent functions by getting a user's retweeters and
        followers
        """
        self.user_dict = {}
        self.user_dict["user_id"] = user_id

        user_retweeters = self.get_user_retweeters(user_id)
        self.user_dict["tweets"] = user_retweeters
        
        followers = self.get_followers(user_id)
        self.user_dict["followers"] = followers

        self.store_user_data()