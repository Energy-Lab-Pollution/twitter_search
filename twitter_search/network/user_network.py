"""
Script to pull tweets and retweeters from a particular user,
"""
import asyncio
import time

import twikit


# from config_utils.constants import TWIKIT_COOKIES_DIR
user_id = "1652537276"
TWIKIT_COOKIES_DIR = "twitter_search/config_utils/cookies.json"


client = twikit.Client("en-US")
client.load_cookies(TWIKIT_COOKIES_DIR)



class UserNetwork:

    def __init__(self):
        self.client = twikit.Client("en-US")
        self.client.load_cookies(TWIKIT_COOKIES_DIR)


    @staticmethod
    def parse_retweeters(retweeters):
        """
        Parse retweeters (user objects) and
        put them into a list of dictionaries
        """
        retweeters_list = []

        if retweeters:
            for retweeter in retweeters:
                retweeter_dict = {}
                retweeter_dict["user_id"] = retweeter.id
                retweeter_dict["username"] = retweeter.screen_name
                retweeter_dict["description"] = retweeter.description
                retweeter_dict["location"] = retweeter.location
                retweeter_dict["followers_count"] = retweeter.followers_count
                retweeter_dict["following_count"] = retweeter.following_count

                retweeters_list.append(retweeter_dict)

        return retweeters_list


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
        time.sleep(2)
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
        
        user_followers = await client.get_followers(user_id)

        if user_followers:
            pass


if __name__ == "__main__":
    pass
    # user_retweeters = asyncio.run(get_retweeters_followers(client, user_id))
    # print(user_retweeters)
