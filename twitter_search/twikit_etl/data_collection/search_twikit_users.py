"""
Pipeline to search twikit users
"""

import asyncio
import json

from twikit import Client


client = Client("en-US")


def parse_tweets_and_users(tweets):
    """
    Parses tweets and users from twikit
    """
    tweets_list = []
    users_list = []
    for tweet in tweets:
        tweet_dict = {}
        tweet_dict["tweet_id"] = tweet.id
        tweet_dict["text"] = tweet.text
        tweet_dict["created_at"] = tweet.created_at
        tweet_dict["author_id"] = tweet.user.id

        user_dict = {}
        user_dict["user_id"] = tweet.user.id
        user_dict["username"] = tweet.user.name
        user_dict["description"] = tweet.user.description
        user_dict["location"] = tweet.user.location
        user_dict["name"] = tweet.user.screen_name
        user_dict["url"] = tweet.user.url
        user_dict["tweets"] = [tweet.text]
        user_dict["geo_code"] = []

        tweets_list.append(tweet_dict)
        users_list.append(user_dict)

        return tweets_list, users_list



async def main():
    # Asynchronous client methods are coroutines and
    # must be called using `await`.

    client.load_cookies("twitter_search/config_utils/cookies.json")

    # Search Latest Tweets
    tweets = await client.search_tweet("New York AND Pollution", "Latest", count=100)
    tweets_list, users_list = parse_tweets_and_users(tweets)

    print(tweets_list)
