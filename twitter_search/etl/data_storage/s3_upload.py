"""
This script will upload the resulting files to AWS
"""

import logging
import os
import time
from pathlib import Path

# Global imports
import boto3
import botocore
from tqdm import tqdm


# Local imports
BUCKET_NAME = "global-rct-users"
REGION_NAME = "us-west-1"
FOLDERS = [
    # "raw_data",
    # "cleaned_data",
    # "analysis_outputs",
    # "twikit_raw_data",
    # "twikit_cleaned_data",
    "networks",
]


# Set logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


s3_client = boto3.client("s3", region_name=REGION_NAME)


def upload_to_s3(local_filename, s3_filename):
    """
    Uploads a given file to S3
    """

    try:
        s3_client.upload_file(
            Filename=local_filename, Bucket=BUCKET_NAME, Key=s3_filename
        )

    except botocore.exceptions.ClientError:
        logger.info("Upload unsuccessful")


def upload_directory(directory_path):
    """
    Uploads all the files in a single
    directory to S3
    """
    # Path of interest is from element 10 onwards
    PATH_INDEX = 10
    # Took this bit from StackOverflow:
    # https://stackoverflow.com/questions/52338706/isadirectoryerror-errno-21-is-a-directory-it-is-a-file
    data_paths = [
        os.path.join(path, file)
        for path, dirs, files in os.walk(directory_path)
        for file in files
    ]

    # Should either be raw_data or clean_data
    for data_path in tqdm(data_paths):
        split_path = data_path.split("/")
        s3_path = "/".join(split_path[PATH_INDEX:])
        upload_to_s3(data_path, s3_path)


if __name__ == "__main__":
    directories = [
        Path(__file__).parent.parent.parent / f"data/{folder}"
        for folder in FOLDERS
    ]
    for directory in directories:
        print(f"Uploading {directory} directory...")
        upload_directory(directory)
        time.sleep(2)
