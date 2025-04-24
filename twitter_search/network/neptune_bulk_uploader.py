"""
Chat GPT generated script to transform JSONs graph data into
csvs and upload to Neptune
"""

import csv
import json
from pathlib import Path

import boto3
import requests

# Local imports
from config_utils.constants import (
    IAM_ROLE_ARN,
    NEPTUNE_AWS_REGION,
    NEPTUNE_ENDPOINT,
    S3_BUCKET,
)


class NeptuneBulkUploader:

    def __init__(self, location, interaction_type):
        # AWS clients
        self.s3_client = boto3.client("s3")
        self.location = location
        self.interaction_type = interaction_type
        self.base_dir = Path(__file__).parent.parent / "data/"
        self.json_path = (
            self.base_dir
            / f"networks/{location}/{interaction_type}_interactions.json"
        )
        self.vertices_csv_path = (
            self.base_dir
            / f"networks/{location}/{location}_{interaction_type}_vertices.csv"
        )
        self.edges_csv_path = (
            self.base_dir
            / f"networks/{location}/{location}_{interaction_type}_edges.csv"
        )
        self.s3_path = f"networks/{location}/neptune/{interaction_type}"

    def convert_json_to_csv(self):
        """
        Convert a JSON edge list into two CSVs: one for vertices and one for edges.

        Args:
            json_path (str): Path to the input JSON file.
            vertices_csv_path (str): Output path for the vertices CSV.
            edges_csv_path (str): Output path for the edges CSV.
            graph_type (str): 'retweet' or 'follow'. Determines edge properties.
            location (str): Location tag to add to all records.
        """
        with open(self.json_path, "r") as f:
            data = json.load(f)

        # Dictionaries to aggregate vertices and edges
        vertices = {}
        edges = {}

        for record in data["edges"]:
            src = record["source"]
            tgt = record["target"]

            # Add or update vertices
            vertices[src] = {
                "username": record["source_username"],
                "followers": record["source_followers"],
            }
            vertices[tgt] = {
                "username": record["target_username"],
                "followers": record["target_followers"],
            }

            # Edge key for aggregation
            key = (src, tgt)
            if self.interaction_type == "retweet":
                tweet_id = record["tweet_id"]
                if key not in edges:
                    edges[key] = {"weight": 0, "tweet_ids": []}
                edges[key]["weight"] += 1
                edges[key]["tweet_ids"].append(tweet_id)
            else:  # follow
                if key not in edges:
                    edges[key] = {}

        # Write vertices CSV
        with open(self.vertices_csv_path, "w", newline="") as vfile:
            writer = csv.DictWriter(
                vfile,
                fieldnames=[
                    "~id",
                    "~label",
                    "username",
                    "followers",
                    "location",
                ],
            )
            writer.writeheader()
            for vid, props in vertices.items():
                writer.writerow(
                    {
                        "~id": vid,
                        "~label": "user",
                        "username": props["username"],
                        "followers": props["followers"],
                        "location": self.location,
                    }
                )

        # Write edges CSV
        if self.interaction_type == "retweet":
            fieldnames = [
                "~id",
                "~from",
                "~to",
                "~label",
                "weight",
                "tweet_ids:list",
                "location",
            ]
        else:
            fieldnames = ["~id", "~from", "~to", "~label", "location"]

        with open(self.edges_csv_path, "w", newline="") as efile:
            writer = csv.DictWriter(efile, fieldnames=fieldnames)
            writer.writeheader()
            for idx, ((src, tgt), props) in enumerate(edges.items(), start=1):
                row = {
                    "~id": f"e{idx}",
                    "~from": src,
                    "~to": tgt,
                    "~label": (
                        "retweeted"
                        if self.interaction_type == "retweet"
                        else "follows"
                    ),
                    "location": self.location,
                }
                if self.interaction_type == "retweet":
                    row["weight"] = props["weight"]
                    # join tweet_ids with semicolon
                    row["tweet_ids:list"] = ";".join(props["tweet_ids"])
                writer.writerow(row)

    def upload_to_s3(self, file_path, s3_key):
        """
        Upload a file to the specified S3 bucket and key.
        """
        self.s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"Uploaded {file_path} to s3://{S3_BUCKET}/{s3_key}")

    @staticmethod
    def bulk_load_to_neptune(s3_prefix):
        """
        Trigger Neptune Bulk Loader for all files under the given S3 prefix.
        """
        neptune_client = boto3.client('neptune-data', region_name='us-east-2')
        # loader_endpoint = f"https://{NEPTUNE_ENDPOINT}:8182/loader"
        response = neptune_client.start_loader_job(
            source= f"s3://{S3_BUCKET}/{s3_prefix}",
            format="csv",
            iamRoleArn=IAM_ROLE_ARN,
            region=NEPTUNE_AWS_REGION,
            failOnError=True,
            parallelism="MEDIUM",
        )
        print("Loader job ID:", response['payload']['jobId'])  # monitor this job via get_loader_job_status


    def run(self):
        """
        Uploads the corresponding csv's from a given location and
        interaction type to Neptune
        """

        self.convert_json_to_csv()
        # Upload vertices
        self.upload_to_s3(
            self.vertices_csv_path,
            f"{self.s3_path}/{self.location}_{self.interaction_type}_vertices.csv",
        )
        # Upload edges
        self.upload_to_s3(
            self.edges_csv_path,
            f"{self.s3_path}/{self.location}_{self.interaction_type}_edges.csv",
        )
        self.bulk_load_to_neptune(self.s3_path)
