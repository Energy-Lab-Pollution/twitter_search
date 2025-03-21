"""
Script to pull tweets and retweeters from a particular user,
"""
import asyncio
import time

import twikit


# from config_utils.constants import TWIKIT_COOKIES_DIR

city = "Kolkata"
user_id = "1652537276"
TWIKIT_COOKIES_DIR = "twitter_search/config_utils/cookies.json"


client = twikit.Client("en-US")
client.load_cookies(TWIKIT_COOKIES_DIR)


async def get_user_retweeters(client, user_id):
    """
    Get a user by id and then get his tweets

    Args
    -------
        - client: Twikit client obj

    Returns:
        - user_tweets:
    """
    user_tweets = await client.get_user_tweets(user_id, "Tweets")
    time.sleep(2)
    dict_list = []
    for tweet in user_tweets[:10]:
        tweet_dict = {}
        tweet_id = tweet.id
        retweeters = await tweet.get_retweeters()
        retweeters = parse_retweeters(retweeters)

        tweet_dict[tweet_id] = retweeters
        dict_list.append(tweet_dict)

    return dict_list


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


if __name__ == "__main__":
    user_retweeters = asyncio.run(get_user_retweeters(client, user_id))
