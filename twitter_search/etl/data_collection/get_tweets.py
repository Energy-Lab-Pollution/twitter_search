"""
This module defines the `TweetGetter` class, which is responsible for retrieving tweets from specified users.
It includes methods to load user data from an input file, fetch tweets using a configured client, and store
the results in an output file.
"""

import time
from datetime import datetime

from config_utils import util
from config_utils.constants import (
    EXPANSIONS,
    MAX_TWEETS_FROM_USERS,
    TWEET_FIELDS,
    USER_FIELDS,
)


class TweetGetter:
    """
    This class is in charge of getting tweets for any user.
    """

    def __init__(self, location, input_file, output_file):
        self.location = location
        self.input_file = input_file
        self.output_file = output_file
        self.MAX_RESULTS = MAX_TWEETS_FROM_USERS
        self.client = util.client_creator()
        # need to adjust this and put it in the constants file.
        self.COUNT_THRESHOLD_TWEETS = 2

    def gettweets_fromusers(self, users_list):
        """
        Get tweets from users.

        Parameters
        ----------
        users_list : list
            List of users to get tweets from.
        """
        count = 0
        for user in users_list:
            max_results = min(user["tweet_count"], self.MAX_RESULTS)

            try:
                response_user_tweets = self.client.get_users_tweets(
                    id=user["user_id"],
                    expansions=EXPANSIONS,
                    tweet_fields=TWEET_FIELDS,
                    max_results=max_results,
                    user_fields=USER_FIELDS,
                )
            except Exception as e:
                print(f"Error fetching tweets for user {user['user_id']}: {e}")
                continue

            if not response_user_tweets.data:
                print(f"No tweets found for user {user['user_id']}")
                continue

            user["tweets"].extend(response_user_tweets.data)
            print(
                f"Retrieved {len(response_user_tweets.data)}\
                   tweets for user {user['user_id']}"
            )
            count += 1
            # FOR TESTING BREAKING ON ONLY 2 loops
            if count >= self.COUNT_THRESHOLD_TWEETS:
                break
            print("Reached threshold, waiting for 15 minutes")
            for time_block in range(1, 4):
                time.sleep(300)
                print(
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {time_block * 5} minutes done out of 15"
                )
            count = 0

        self.total_users_dict = users_list

    def load_users(self):
        """
        Load and preprocess the list of users from the input JSON file.

        Returns
        -------
        list
            List of users.
        """
        users_list_raw = util.load_json(self.input_file)
        users_list = util.flatten_and_remove_empty(users_list_raw)
        return users_list

    def store_users(self):
        """
        Convert the user list to a JSON and store it in the output file.
        """
        util.json_maker(self.output_file, self.total_users_dict)
        print(f"Stored data for {len(self.total_users_dict)} users")

    def get_users_tweets(self):
        """
        Reads lists of users from a JSON file, fetches their tweets,
        and stores the results.
        """
        try:
            users_list = self.load_users()
            print(f"Obtaining tweets for {len(users_list)} users")
            self.gettweets_fromusers(users_list)
            self.store_users()
        except Exception as e:
            print(f"An error occurred: {e}")
