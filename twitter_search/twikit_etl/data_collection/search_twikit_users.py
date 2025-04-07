"""
Pipeline to search twikit users
"""
import asyncio
from datetime import datetime

import twikit
from config_utils import constants, util


class TwikitUserSearcher:
    def __init__(
        self,
        output_file_users,
        output_file_tweets,
        twikit_threshold,
        query=None,
    ):
        self.output_file_users = output_file_users
        self.output_file_tweets = output_file_tweets
        self.query = query

        # Threshold for this particular city
        self.twikit_threshold = twikit_threshold

        self.client = twikit.Client("en-US")
        self.client.load_cookies(constants.TWIKIT_COOKIES_DIR)

    @staticmethod
    def convert_to_yyyy_mm_dd(date_string):
        """
        Converts a date string in the format "Fri Dec 06 18:09:05 +0000 2024"
        to the "yyyy-mm-dd" format.

        Args:
            date_string: The input date string.

        Returns:
            The date string in the "yyyy-mm-dd" format.
        """
        try:
            date_obj = datetime.strptime(date_string, "%a %b %d %H:%M:%S %z %Y")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            print(f"Invalid date format: {date_string}")
            return None

    def parse_tweets_and_users(self, tweets):
        """
        Parses tweets and users from twikit into dictionaries

        Args:
            tweets: Array of twikit.tweet.Tweet objects

        Returns:
            tweets_list: Array of dictionaries with tweets' data
            users_list: Array of dictionaries with users' data
        """
        tweets_list = []
        users_list = []
        for tweet in tweets:
            tweet_dict = {}
            tweet_dict["tweet_id"] = tweet.id
            tweet_dict["text"] = tweet.text
            tweet_dict["created_at"] = tweet.created_at
            tweet_dict["author_id"] = tweet.user.id

            parsed_date = self.convert_to_yyyy_mm_dd(tweet.created_at)

            user_dict = {}
            user_dict["user_id"] = tweet.user.id
            user_dict["username"] = tweet.user.name
            user_dict["description"] = tweet.user.description
            user_dict["location"] = tweet.user.location
            user_dict["name"] = tweet.user.screen_name
            user_dict["url"] = tweet.user.url
            user_dict["followers_count"] = tweet.user.followers_count
            user_dict["following_count"] = tweet.user.following_count
            user_dict["listed_count"] = tweet.user.listed_count
            user_dict["tweets"] = [tweet.text]
            user_dict["tweet_date"] = parsed_date
            user_dict["user_date_id"] = f"{tweet.user.id}-{parsed_date}"
            user_dict["geo_code"] = []

            tweets_list.append(tweet_dict)
            users_list.append(user_dict)

        return tweets_list, users_list

    async def search_tweets_and_users(self):
        """
        Method used to search for tweets, with twikit
        with the given query

        This method uses twikit's "await next" function
        to get more tweets with the given query.
        """

        tweets = await self.client.search_tweet(
            self.query, "Latest", count=1000
        )
        self.tweets_list, self.users_list = self.parse_tweets_and_users(tweets)

        more_tweets_available = True
        num_iter = 1

        next_tweets = await tweets.next()
        if next_tweets:
            next_tweets_list, next_users_list = self.parse_tweets_and_users(
                next_tweets
            )
            self.tweets_list.extend(next_tweets_list)
            self.users_list.extend(next_users_list)
        else:
            more_tweets_available = False

        while more_tweets_available:
            next_tweets = await next_tweets.next()
            if next_tweets:
                next_tweets_list, next_users_list = self.parse_tweets_and_users(
                    next_tweets
                )
                self.tweets_list.extend(next_tweets_list)
                self.users_list.extend(next_users_list)

            else:
                more_tweets_available = False

            if num_iter % 5 == 0:
                print(f"Processed {num_iter} batches")

            if num_iter == self.twikit_threshold:
                break

            num_iter += 1

    def store_users_and_tweets(self):
        """
        Stores users and tweets into the provided path

        The util function checks for any existing dictionaries and
        only adds the newer data
        """
        util.json_maker(self.output_file_users, self.users_list)
        util.json_maker(self.output_file_tweets, self.users_list)

    def run_search(self):
        """
        Runs the entire search pipeline
        """
        try:
            asyncio.run(self.search_tweets_and_users())
        except twikit.errors.TooManyRequests:
            print("Too many requests, stopping...")

        if not hasattr(self, "users_list"):
            return
        self.store_users_and_tweets()
