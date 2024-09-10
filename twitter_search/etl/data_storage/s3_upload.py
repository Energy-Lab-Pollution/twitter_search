"""
This script will upload the resulting files to AWS
"""

import logging

# Global imports
import boto3
import botocore

# Local imports
from config_utils.constants import BUCKET_NAME, REGION_NAME


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
