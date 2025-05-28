"""
Script to upload all pending tweets from the users in the pilot cities
"""
import datetime
import json
from argparse import ArgumentParser
from pathlib import Path

import boto3
import botocore
from config_utils.constants import (
    NEPTUNE_S3_BUCKET,
    REGION_NAME,
)
from config_utils.util import (
    client_creator,
    convert_to_iso_format,
)


def insert_tweets_to_s3(self, user_id, tweets_list):
    """
    Function to insert each user's tweets as
    a txt files to S3

    These tweets are already filtered
    """
    s3_client = boto3.client("s3", region_name=REGION_NAME)
    for tweet in tweets_list:
        s3_path = f"networks/{self.location}/classification/{user_id}/input/tweet_{tweet['tweet_id']}.txt"
        try:
            s3_client.put_object(
                Bucket=NEPTUNE_S3_BUCKET,
                Key=s3_path,
                Body=tweet["tweet_text"].encode("utf-8", errors="ignore"),
            )
        except botocore.exceptions.ClientError:
            print(f"Unable to upload {tweet['tweet_id']} for {user_id}")
            continue