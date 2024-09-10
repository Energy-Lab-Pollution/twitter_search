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

# Local imports
BUCKET_NAME = "global-rct-users"
REGION_NAME = "us-west-1"
FOLDERS = ["raw_data", "cleaned_data"]


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
    files = os.listdir(directory_path)

    # Should either be raw_data or clean_data
    s3_folder = str(directory_path).split("/")[-1]

    if s3_folder not in FOLDERS:
        raise FileNotFoundError(
            f"Desired folder is not one of {FOLDERS} - please check"
        )

    for file in files:
        upload_to_s3(f"{directory_path}/{file}", f"{s3_folder}/{file}")

    logger.info(f"Done uploading files to S3 folder: {s3_folder}")


if __name__ == "__main__":

    raw_dir = Path(__file__).parent.parent.parent / "data/raw_data"
    clean_dir = Path(__file__).parent.parent.parent / "data/cleaned_data"

    upload_directory(raw_dir)
    upload_directory(clean_dir)
