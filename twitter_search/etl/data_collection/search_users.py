"""
Module for searching users on Twitter based on a query and location.

Author : Praveen Chandar and Federico Dominguez Molina
"""

from config_utils import util, constants


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

    def __init__(self, location, output_file_users, output_file_tweets, query=None):
        if query is None:
            self.query = self.query_builder(location)
        else:
            self.query = query
        self.location = location
        self.total_tweets = []
        self.total_users = []
        self.twitter_client = util.client_creator()

        self.output_file_user = output_file_users
        self.output_file_tweets = output_file_tweets

        print("Clients initiated")

    def query_builder(self, location):

        return f"(air pollution {location} OR {location} air OR {location} \
            pollution OR {location} public health OR bad air {location} OR \
            {location} asthma OR {location} polluted OR pollution control board) \
            (#pollution OR #environment OR #cleanair OR #airquality) -is:retweet"

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
            count = min(100, MAX_RESULTS - result_count)
            response = self.twitter_client.search_recent_tweets(
                query=self.query,
                max_results=count,
                next_token=next_token,
                expansions=EXPANSIONS,
                tweet_fields=TWEET_FIELDS,
                user_fields=USER_FIELDS,
            )
            result_count += response.meta["result_count"]
            self.total_tweets.extend(response.data)
            self.total_users.extend(response.includes["users"])
            try:
                next_token = response.meta["next_token"]
            except:
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
            self.total_users_dict = util.user_dictmaker(self.total_users)
            self.total_tweets_dict = util.tweet_dictmaker(self.total_tweets)
        except Exception as e:
            print(f"An error occurred: {e}")

    def store_users(self):
        """
        convert the user list to a json and store it.

        Args:
            None

        Returns:
            None
        """
        util.json_maker(self.output_file_user, self.total_users_dict)
        print("Total number of users:", len(self.total_users))

    def store_tweets(self):
        """
        convert the tweet list to a json and store it.

        Args:
            None

        Returns:
            None
        """
        util.json_maker(self.output_file_tweets, self.total_tweets_dict)
        print("Total number of tweets:", len(self.total_tweets_dict))

    def run_search_all(self):
        self.search_users_tweets()
        self.store_users()
        self.store_tweets()
