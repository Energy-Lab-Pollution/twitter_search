"""
Script to upload all pending tweets from the users in the pilot cities to S3
"""
from argparse import ArgumentParser
from pathlib import Path
from tqdm import tqdm

import boto3
import botocore
from config_utils.constants import (
    NEPTUNE_S3_BUCKET,
    REGION_NAME,
)
from config_utils.util import (
    load_json
)


def insert_tweets_to_s3(location, user_id, tweets_list):
    """
    Function to insert each user's tweets as
    a txt files to S3

    These tweets are already filtered
    """
    if not tweets_list:
        print(f"No tweets to upload for user {user_id}")
        return
    for tweet in tqdm(tweets_list):
        location = location.replace(" ", "_")
        s3_path = f"networks/{location}/classification/{user_id}/input/tweet_{tweet['tweet_id']}.txt"
        try:
            s3_client.put_object(
                Bucket=NEPTUNE_S3_BUCKET,
                Key=s3_path,
                Body=tweet["tweet_text"].encode("utf-8", errors="ignore"),
            )
        except botocore.exceptions.ClientError:
            print(f"Unable to upload {tweet['tweet_id']} for {user_id}")
            continue


def upload_user_tweets(location):
    """
    Reads user data from raw location JSON, then uploads
    tweets to S3
    """
    # Paths setup
    location = location.lower()
    file_path = Path(__file__).parent / "data/" / f"networks/{location}/{location}.json"
    location_json = load_json(file_path)

    num_users = len(location_json)
    print(f"Number of root users {num_users}")

    for user_dict in tqdm(location_json):
        original_tweets = []
        if "followers_count" not in user_dict:
            print(f"Skipping {user_dict['user_id']}")
            continue

        user_tweets = user_dict["tweets"]
        for user_tweet in user_tweets:
            # Remove reposts by others
            if not user_tweet["tweet_text"].startswith("RT @"):
                original_tweets.append(user_tweet)
        
        insert_tweets_to_s3(location, user_dict['user_id'], original_tweets)
    
    print(f"Done with {location}")


if __name__ == "__main__":

    parser = ArgumentParser(description="Specify params to upload tweets to S3")
    parser.add_argument("--location", type=str, help="Location to read tweets from")
    args = parser.parse_args()

    s3_client = boto3.client("s3", region_name=REGION_NAME)
    upload_user_tweets(args.location)