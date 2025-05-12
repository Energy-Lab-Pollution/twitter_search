"""
Script to get a given user's tweets
"""

import datetime
import json
from argparse import ArgumentParser

import boto3
import twikit
from config_utils.constants import (
    EXPANSIONS,
    MAX_RESULTS,
    TWEET_FIELDS,
    TWIKIT_COOKIES_DIR,
    TWIKIT_TWEETS_THRESHOLD,
    USER_FIELDS,
)
from config_utils.util import (
    check_location,
    client_creator,
    convert_to_iso_format,
)


class UserTweets:
    def __init__(self, location, tweet_count):
        self.location = location
        self.sqs_client = boto3.client("sqs")
        self.tweet_count = tweet_count
