"""
Classifies a user by using their tweets and description
"""

import random
from concurrent.futures import ThreadPoolExecutor

import boto3
import botocore
from constants import (
    GEMINI_MODEL,
    NEPTUNE_AWS_REGION,
    NEPTUNE_S3_BUCKET,
    OPENAI_MODEL,
)

# Local imports
from gemini_classifier import GeminiClassifier
from openai_classifier import GPTClassifier
from tqdm import tqdm


s3_client = boto3.client("s3", region_name=NEPTUNE_AWS_REGION)


def list_user_objects(bucket, prefix, user_dir=True):
    """
    Return a list of all 'sub-folder' prefixes under the given prefix,
    even if there are more than 1,000.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")

    user_dirs = []
    for page in pages:
        if user_dir:
            for cp in page.get("CommonPrefixes", []):
                user_dir = f"{cp['Prefix']}input/"
                user_dirs.append(user_dir)
        # Get user tweets and description
        else:
            for element in page.get("Contents", []):
                user_dirs.append(element["Key"])

    return user_dirs


def extract_text(filename):
    """
    Extracts the content of an s3 file

    Args:
        - filename (str): path to txt file in S3

    Returns:
        - string (str): File content
    """
    try:
        object = s3_client.get_object(Bucket=NEPTUNE_S3_BUCKET, Key=filename)
        string = object["Body"].read().decode("utf-8")
    except botocore.exceptions.ClientError as error:
        print(f"Error for {filename} - {error}")
        string = ""

    return string


def extract_several_files(file_keys, max_workers=8):
    """
    Reads content from an array of files by using multiprocessing

    Args:
        file_keys (str): list with file names in S3
        max_workers (int): max number of workers to use
    Returns:
        tweets_list (list)
    """
    # Submit all jobs
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        # executor.map yields results in the same order as file_keys
        for content in tqdm(
            pool.map(extract_text, file_keys),
            total=len(file_keys),
            desc="Reading from S3",
        ):
            results.append(content)

    return results


def process_and_classify_user(user_prefix, gemini_classifier, gpt_classifier):
    """
    Given a user's directory:
        - get and process her description and their tweets
        - classify them
    """
    user = user_prefix.split("/")[3]
    user_content = list_user_objects(
        NEPTUNE_S3_BUCKET, user_prefix, user_dir=False
    )
    description_text = extract_text(f"{user_prefix}description.txt")
    print(f"User id {user} - {description_text}")
    if len(user_content) > 1:
        tweets_list = extract_several_files(user_content[1:])

    user_tweets_str = "\n".join(tweets_list) if len(user_content) > 1 else ""
    gemini_classifier.send_prompt(description_text, user_tweets_str)
    gpt_classifier.send_prompt(description_text, user_tweets_str)

    print(f"Gemini classification: {gemini_classifier.content}")
    print(f"GPT Classifier: {gpt_classifier.content}")

    # TODO: Adding classification result to user attributes in neptune


if __name__ == "__main__":

    gemini_classifier = GeminiClassifier(model=GEMINI_MODEL)
    gpt_classifier = GPTClassifier(model=OPENAI_MODEL)
    city = "kolkata"
    city_prefix = f"networks/{city}/classification/"
    all_user_prefixes = list_user_objects(NEPTUNE_S3_BUCKET, city_prefix)
    print(f"Found {len(all_user_prefixes)} user folders.")
    random_user = random.choice(all_user_prefixes)

    # for random_user in all_user_prefixes[:1]:
    process_and_classify_user(random_user, gemini_classifier, gpt_classifier)
