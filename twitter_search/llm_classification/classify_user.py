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

def extract_dataframe(filename):
    """
    Parses a dataframe from S3
    """
    try:
        object = s3_client.get_object(Bucket=NEPTUNE_S3_BUCKET, Key=filename)
        print("Getting .txt file")
        csv_string = object["Body"].read().decode("utf-8")
        # df = pd.read_csv(io.StringIO(csv_string))

    except botocore.exceptions.ClientError as error:
        print(f"Error for {filename} - {error}")
        csv_string = ""

    return csv_string
