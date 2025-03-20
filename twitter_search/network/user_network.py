"""
Script to pull tweets and retweeters from a particular user, 
"""
import twikit
import asyncio

from config_utils.constants import TWIKIT_COOKIES_DIR

city = "Kolkata"
user_id = "1652537276"

client = twikit.Client("en-US")
client.load_cookies(TWIKIT_COOKIES_DIR)

async def get_users_tweets(client, user_id):
    """
    Get a user by id and then get his tweets

    Args
    -------
        - client: Twikit client obj

    Returns:
        - user_tweets: 
    """
    user_tweets = await client.get_user_tweets(user_id)
    
    return user_tweets

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

            retweeters_list.append(retweeter_dict)

    return retweeters_list


def get_users_retweeters(tweets):
    """
    Gets a list of retweeters given a list
    of tweets

    Args:
        - tweets: List of twikit.tweet objects
    """
    dict_list = []
    for tweet in tweets:
        tweet_dict = {}
        tweet_id = tweet.id 
        retweeters = tweet.get_retweeters()

        tweet_dict[tweet_id] = retweeters
        dict_list.append(tweet_dict)

    return dict_list

    

if __name__ == "__main__":

    user_tweets = asyncio.run()







