"""
Script to upload all pending tweets from the users in the pilot cities to S3
"""
from argparse import ArgumentParser
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import botocore
from config_utils.constants import (
    NEPTUNE_S3_BUCKET,
    REGION_NAME,
    NUM_WORKERS
)
from config_utils.util import (
    load_json
)

def _upload_one(args):
    """
    Put a single object in S3
    """
    s3_client.put_object(**args)

def insert_descriptions_to_s3(location, users_list):
    """
    Function to insert each user's description as
    a txt file to S3
    """
    s3_client = boto3.client("s3", region_name=REGION_NAME)
    for user in tqdm(users_list):
        s3_path = f"networks/{location}/classification/{user['user_id']}/input/description.txt"
        try:
            s3_client.put_object(
                Bucket=NEPTUNE_S3_BUCKET,
                Key=s3_path,
                Body=user["description"].encode("utf-8", errors="ignore"),
            )
        except botocore.exceptions.ClientError:
            print(f"Unable to upload description for {user['user_id']}")
            continue


def insert_tweets_to_s3(location, user_id, tweets_list):
    """
    Function to insert each user's tweets as
    a txt files to S3

    These tweets are already filtered
    """
    if not tweets_list:
        return

    jobs = []
    for t in tweets_list:
        key = f"networks/{location}/classification/{user_id}/input/tweet_{t['tweet_id']}.txt"
        jobs.append({
            "Bucket": NEPTUNE_S3_BUCKET,
            "Key":    key,
            "Body":   t["tweet_text"].encode("utf-8", errors="ignore"),
        })

    # Parallely uploading tweets
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
        futures = [pool.submit(_upload_one, job) for job in jobs]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Uploading tweets"):
            try:
                future.result()
            except Exception as e:
                # handle/log individual upload errors
                print("Upload failed:", e)

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
    users_list = []
    print(f"Number of root users {num_users}")

    for user_dict in tqdm(location_json):
        tmp_dict = {}
        original_tweets = []
        if "followers_count" not in user_dict:
            print(f"Skipping {user_dict['user_id']}")
            continue

        tmp_dict['user_id'] = user_dict['user_id']
        tmp_dict['description'] = user_dict['description']
        users_list.append(tmp_dict)

        user_tweets = user_dict["tweets"]
        for user_tweet in user_tweets:
            # Remove reposts by others
            if not user_tweet["tweet_text"].startswith("RT @"):
                original_tweets.append(user_tweet)
        
        insert_tweets_to_s3(location, user_dict['user_id'], original_tweets)

    print("Inserting descriptions...")
    insert_descriptions_to_s3(location, users_list)

    print(f"Done with {location}")


if __name__ == "__main__":

    parser = ArgumentParser(description="Specify params to upload tweets to S3")
    parser.add_argument("--location", type=str, help="Location to read tweets from")
    args = parser.parse_args()

    s3_client = boto3.client("s3", region_name=REGION_NAME)
    print("Inserting tweets...")
    upload_user_tweets(args.location)
