"""
Twikit testing script
"""

# import asyncio

import asyncio
from twikit import Client


client = Client("en-US")


async def main():
    # Asynchronous client methods are coroutines and
    # must be called using `await`.
    # await client.login(
    #     auth_info_1=USERNAME, auth_info_2=EMAIL, password=PASSWORD
    # )

    client.load_cookies("twitter_search/config_utils/cookies.json")

    ###########################################

    # Search Latest Tweets
    tweets = await client.search_tweet("New York AND Pollution", "Latest")
    for tweet in tweets:
        print(tweet.text)

    # # Search more tweets
    # more_tweets = await tweets.next()

if __name__ == "__main__":
    asyncio.run(main())