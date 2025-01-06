"""
Pipeline to search twikit users
"""

import asyncio
import json

from twikit import Client

QUERY = """location ((air pollution) OR pollution OR (public health)
                OR (poor air) OR asthma OR polluted OR (pollution control board)
                OR smog OR (air quality)) -is:retweet"""


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

    client.load_cookies("../../twitter_search/config_utils/cookies.json")

    # Search Latest Tweets
    query = QUERY.replace("location", "New York")
    tweets = await client.search_tweet(query, "Latest", count=20)
    tweets_list, users_list = parse_tweets_and_users(tweets)
    print(tweets_list)

    more_tweets_available = True
    num_iter = 1


    next_tweets = await tweets.next()
    if next_tweets:
        next_tweets_list, next_users_list = parse_tweets_and_users(next_tweets)
        tweets_list.extend(next_tweets_list)
        users_list.extend(next_users_list)
    else:
        more_tweets_available = False

    while more_tweets_available:
        next_tweets = await next_tweets.next()
        if next_tweets:
            next_tweets_list, next_users_list = parse_tweets_and_users(next_tweets)
            tweets_list.extend(next_tweets_list)
            users_list.extend(next_users_list)

        else:
            more_tweets_available = False

        num_iter += 1

        


    


if __name__ == "__main__":
    asyncio.run(main())