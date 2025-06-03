"""
Classifies a user by using their tweets and description
"""

import boto3
import botocore

# Local imports
from gemini_classifier import GeminiClassifier
from openai_classifier import GPTAClassifier
from constants import NEPTUNE_AWS_REGION, NEPTUNE_S3_BUCKET

s3_client = boto3.client('s3', region_name=NEPTUNE_AWS_REGION)

def list_user_folders(bucket, prefix):
    """
    Return a list of all “sub‐folder” prefixes under the given prefix,
    even if there are more than 1,000.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter="/"
    )

    user_dirs = []
    for page in pages:
        # page["CommonPrefixes"] could be up to 1,000 per page
        for cp in page.get("CommonPrefixes", []):
            user_dirs.append(cp["Prefix"])

    return user_dirs

def extract_text(filename):
    """
    Parses a dataframe from S3
    """
    try:
        object = s3_client.get_object(Bucket=NEPTUNE_S3_BUCKET, Key=filename)
        print("Getting .txt file")
        string = object["Body"].read().decode("utf-8")
        # df = pd.read_csv(io.StringIO(csv_string))

    except botocore.exceptions.ClientError as error:
        print(f"Error for {filename} - {error}")
        string = ""

    return string
