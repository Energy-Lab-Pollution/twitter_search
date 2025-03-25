"""
Script to pull tweets and retweeters from a particular user,
"""
import asyncio
import time

import twikit


# from config_utils.constants import TWIKIT_COOKIES_DIR
user_id = "1652537276"
TWIKIT_COOKIES_DIR = "twitter_search/config_utils/cookies.json"


class UserNetwork:
    def __init__(self):
        self.client = twikit.Client("en-US")
        self.client.load_cookies(TWIKIT_COOKIES_DIR)

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

    async def get_retweeters(self, client, user_id):
        """
        For a given user, we extract their retweeters and followers

        Args
        -------
            - client: Twikit client obj

        Returns:
            - user_tweets:
        """

        dict_list = []

        user_tweets = await client.get_user_tweets(user_id, "Tweets")
        for tweet in user_tweets:
            tweet_dict = {}
            tweet_id = tweet.id
            retweeters = await tweet.get_retweeters()
            retweeters = self.parse_retweeters(retweeters)

            tweet_dict[tweet_id] = retweeters
            dict_list.append(tweet_dict)

        return dict_list

    async def get_followers(self, client, user_id):
        """
        Gets a given user's followers
        """

        followers = await client.get_followers(user_id)

        if followers:
            followers = self.parse_users(followers)

        more_followers = await followers.next()
        if more_followers:
            more_followers = self.parse_users(more_followers)


if __name__ == "__main__":
    pass
    # user_retweeters = asyncio.run(get_retweeters_followers(client, user_id))
    # print(user_retweeters)
