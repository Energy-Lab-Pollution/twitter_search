"""
Module for searching users on Twitter based on a query and location.

Author : Praveen Chandar and Federico Dominguez Molina
"""

from datetime import datetime

import pytz
from config_utils import constants, util


class UserSearcher:
    """
    A class for searching users based on location and optional query.

    Attributes:
        location (str): The location for which users are being searched.
        query (str): The optional query string. If not provided, a default
        query is generated based on the location.
        search_tweets_result: Placeholder for storing search results.
        total_users: Placeholder for storing total number of users found.
        client: tweepy client
    """

    def __init__(
        self, location, output_file_users, output_file_tweets, query=None
    ):

        self.query = query
        self.location = location
        self.total_tweets = []
        self.total_users = []
        self.twitter_client = util.client_creator()

        self.output_file_user = output_file_users
        self.output_file_tweets = output_file_tweets
        self.todays_date = datetime.now(pytz.timezone("America/Chicago"))
        self.todays_date_str = datetime.strftime(self.todays_date, "%Y-%m-%d")
        self.date_digits = 10

        print("Clients initiated")


    def search_tweets(self, MAX_RESULTS, EXPANSIONS, TWEET_FIELDS, USER_FIELDS):
        """
        Search for recent tweets based on a query.

        Args:
            client: An authenticated Twitter API client.
            query (str): The search query.
            MAX_RESULTS (int): Maximum number of results to retrieve.
            EXPANSIONS (list): List of expansions to include in the response.
            TWEET_FIELDS (list): List of tweet fields to include in the response.
            USER_FIELDS (list): List of user fields to include in the response.

        Returns:
            dict: The search result containing tweets and associated users.
        """
        result_count = 0
        next_token = None

        # pagination
        while result_count < MAX_RESULTS:
            print(f"Max results is: {result_count}")
            response = self.twitter_client.search_recent_tweets(
                query=self.query,
                max_results=MAX_RESULTS,
                next_token=next_token,
                expansions=EXPANSIONS,
                tweet_fields=TWEET_FIELDS,
                user_fields=USER_FIELDS,
            )
            if response.meta["result_count"] == 0:
                print("No more results found.")
                break
            result_count += response.meta["result_count"]
            self.total_tweets.extend(response.data)
            self.total_users.extend(response.includes["users"])
            try:
                next_token = response.meta["next_token"]
            except Exception as err:
                print(err)
                next_token = None

            if next_token is None:
                break

    def search_users_tweets(self):
        """
        Search for users on Twitter based on a query and location.

        Args:
            query (str): The search query.
            location (str): The location for which to search users.

        Returns:
            None
        """

        try:
            print("Now searching for tweets")
            self.search_tweets(
                constants.MAX_RESULTS,
                constants.EXPANSIONS,
                constants.TWEET_FIELDS,
                constants.USER_FIELDS,
            )

            if not self.total_users:
                print("No users found.")
                return

            self.total_users_dict = util.user_dictmaker(self.total_users)
            self.total_tweets_dict = util.tweet_dictmaker(self.total_tweets)

        except Exception as e:
            print(f"An error occurred: {e}")

    def add_date_to_user(self):
        """
        If there is a date associated to any tweet, add it to the user

        If the user does not have any tweet associated to her, the
        function adds a default datetime string
        """
        # Get authors and dates from the available tweets
        tweet_authors = [tweet["author_id"] for tweet in self.total_tweets_dict]
        tweet_dates = [tweet["created_at"] for tweet in self.total_tweets_dict]

        # Dictionary of dates and authors
        authors_dates_dict = dict(zip(tweet_authors, tweet_dates))

        # Add such date to the collected users
        for user_dict in self.total_users_dict:
            user_id = user_dict["user_id"]
            author_date = authors_dates_dict.get(user_id)

            if author_date:
                # Just get 10 digits for year, month and day
                user_dict["tweet_date"] = author_date[: self.date_digits]

            else:
                user_dict["tweet_date"] = self.todays_date_str

            user_dict["user_date_id"] = f"{user_id}-{user_dict['tweet_date']}"

    def store_users(self):
        """
        convert the user list to a json and store it.

        Args:
            None

        Returns:
            None
        """
        self.add_date_to_user()
        self.unique_users_dict = util.remove_duplicate_records(
            self.total_users_dict
        )

        util.json_maker(self.output_file_user, self.unique_users_dict)
        print("Total number of users:", len(self.total_users))

    def store_tweets(self):
        """
        convert the tweet list to a json and store it.

        Args:
            None

        Returns:
            None
        """
        self.unique_tweets_dict = util.remove_duplicate_records(
            self.total_tweets_dict
        )
        util.json_maker(self.output_file_tweets, self.unique_tweets_dict)
        print("Total number of tweets:", len(self.unique_tweets_dict))

    def run_search_all(self):
        """
        Runs the entire search pipeline
        """
        self.search_users_tweets()
        if not self.total_users:
            return
        self.store_users()
        self.store_tweets()
