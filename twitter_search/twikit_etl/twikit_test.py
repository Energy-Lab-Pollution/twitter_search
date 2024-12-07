"""
Twikit testing script
"""

# import asyncio

import asyncio
import json

from twikit import Client


client = Client("en-US")


async def main():
    # Asynchronous client methods are coroutines and
    # must be called using `await`.

    client.load_cookies("twitter_search/config_utils/cookies.json")

    # Search Latest Tweets
    tweets = await client.search_tweet("New York AND Pollution", "Latest", count=100)
    tweets_list = []
    users_list = []
    for tweet in tweets:
        tweet_dict = {}
        tweet_dict["tweet_id"] = tweet.id
        tweet_dict["text"] = tweet.text
        tweet_dict["created_at"] = tweet.created_at
        tweet_dict["author_id"] = tweet.user.id

        user_dict = {}
        user_dict["user_id"]: tweet.user.id
        user_dict["username"]: tweet.user.name
        user_dict["description"]: tweet.user.name
        user_dict["location"]: tweet.user.name
        user_dict["name"]: tweet.user.screen_name
        user_dict["url"]: tweet.user.screen_name
        user_dict["tweets"] = [tweet.text]
        user_dict["geo_code"] = []

        tweets_list.append(tweet_dict)
        users_list.append(user_dict)

    # Specify the file name
    tweets_filename = "twikit-tweets-test.json"
    users_filename = "twikit-users-test"

    # Open the file in write mode ('w')
    with open(tweets_filename, "w") as file:
        # Dump the data to the file with indentation for readability
        json.dump(tweets_list, file, indent=4)

        # Open the file in write mode ('w')
    with open(users_filename, "w") as file:
        # Dump the data to the file with indentation for readability
        json.dump(users_list, file, indent=4)

    # Search more tweets
    # more_tweets = await tweets.next()


if __name__ == "__main__":
    asyncio.run(main())
