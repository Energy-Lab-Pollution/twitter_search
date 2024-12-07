"""
Pipeline to search twikit users
"""

import asyncio
import json

from twikit import Client


client = Client("en-US")


async def main():
    # Asynchronous client methods are coroutines and
    # must be called using `await`.

    client.load_cookies("twitter_search/config_utils/cookies.json")

    # Search Latest Tweets
    tweets = await client.search_tweet(
        "New York AND Pollution", "Latest", count=100
    )
    tweets_list = []
    for tweet in tweets:
        tweet_dict = {}
        tweet_dict["tweet_id"] = tweet.id
        tweet_dict["text"] = tweet.text
        tweet_dict["created_at"] = tweet.created_at
        tweet_dict["author_id"] = tweet.user.id

        tweets_list.append(tweet_dict)

    print(tweets_list)

