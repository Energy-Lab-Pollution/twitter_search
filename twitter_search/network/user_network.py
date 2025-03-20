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

async def get_users_tweets(client):
    """
    Get a user by id and then get his tweets

    Args
    -------
        - client: Twikit client obj

    Returns:
        - user_tweets: 
    """
    user_tweets = await client.get_user_tweets()
    
    return user_tweets


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

        tweet_dict[tweet.id] = retweeters
        dict_list.append(tweet_dict)

    return dict_list

    







