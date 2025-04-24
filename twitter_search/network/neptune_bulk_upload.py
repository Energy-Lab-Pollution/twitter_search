"""
Chat GPT generated script to transform JSONs graph data into
csvs and upload to Neptune
"""

import csv
import json

import boto3
import requests
from config_utils.constants import (
    IAM_ROLE_ARN,
    NEPTUNE_AWS_REGION,
    NEPTUNE_ENDPOINT,
    S3_BUCKET,
)


# AWS clients
s3_client = boto3.client("s3")


def convert_json_to_csv(
    json_path, vertices_csv_path, edges_csv_path, graph_type, location
):
    """
    Convert a JSON edge list into two CSVs: one for vertices and one for edges.

    Args:
        json_path (str): Path to the input JSON file.
        vertices_csv_path (str): Output path for the vertices CSV.
        edges_csv_path (str): Output path for the edges CSV.
        graph_type (str): 'retweet' or 'follow'. Determines edge properties.
        location (str): Location tag to add to all records.
    """
    with open(json_path, "r") as f:
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
        if graph_type == "retweet":
            tweet_id = record["tweet_id"]
            if key not in edges:
                edges[key] = {"weight": 0, "tweet_ids": []}
            edges[key]["weight"] += 1
            edges[key]["tweet_ids"].append(tweet_id)
        else:  # follow
            if key not in edges:
                edges[key] = {}

    # Write vertices CSV
    with open(vertices_csv_path, "w", newline="") as vfile:
        writer = csv.DictWriter(
            vfile,
            fieldnames=["~id", "~label", "username", "followers", "location"],
        )
        writer.writeheader()
        for vid, props in vertices.items():
            writer.writerow(
                {
                    "~id": vid,
                    "~label": "user",
                    "username": props["username"],
                    "followers": props["followers"],
                    "location": location,
                }
            )

    # Write edges CSV
    if graph_type == "retweet":
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

    with open(edges_csv_path, "w", newline="") as efile:
        writer = csv.DictWriter(efile, fieldnames=fieldnames)
        writer.writeheader()
        for idx, ((src, tgt), props) in enumerate(edges.items(), start=1):
            row = {
                "~id": f"e{idx}",
                "~from": src,
                "~to": tgt,
                "~label": "retweeted" if graph_type == "retweet" else "follows",
                "location": location,
            }
            if graph_type == "retweet":
                row["weight"] = props["weight"]
                # join tweet_ids with semicolon
                row["tweet_ids:list"] = ";".join(props["tweet_ids"])
            writer.writerow(row)


def upload_to_s3(file_path, s3_key):
    """
    Upload a file to the specified S3 bucket and key.
    """
    s3_client.upload_file(file_path, S3_BUCKET, s3_key)
    print(f"Uploaded {file_path} to s3://{S3_BUCKET}/{s3_key}")


def bulk_load_to_neptune(s3_prefix):
    """
    Trigger Neptune Bulk Loader for all files under the given S3 prefix.
    """
    loader_endpoint = f"https://{NEPTUNE_ENDPOINT}:8182/loader"
    payload = {
        "source": f"s3://{S3_BUCKET}/{s3_prefix}",
        "format": "csv",
        "iamRoleArn": IAM_ROLE_ARN,
        "region": NEPTUNE_AWS_REGION,
        "failOnError": "FALSE",
        "parallelism": "HIGH",
    }
    response = requests.post(loader_endpoint, json=payload)
    print("Bulk load response:", response.status_code, response.json())


if __name__ == "__main__":
    # Example usage:
    # Convert and load retweets
    location = "kolkata"
    interaction_type = "retweet"

    json_path = (
        f"data/networks/{location}/{interaction_type}_interactions.json",
    )
    vertices_csv_path = (
        f"data/networks/{location}_{interaction_type}_vertices.csv",
    )
    edges_csv_path = (
        f"data/networks/{location}/{location}_{interaction_type}_edges.csv"
    )
    s3_path = f"networks/{location}/neptune/{interaction_type}"

    convert_json_to_csv(
        json_path,
        vertices_csv_path,
        edges_csv_path,
        graph_type=interaction_type,
        location=location,
    )
    # Upload vertices
    upload_to_s3(
        vertices_csv_path,
        f"{s3_path}/{location}_{interaction_type}_vertices.csv",
    )
    # Upload edges
    upload_to_s3(
        edges_csv_path,
        f"{s3_path}/kolkata_retweet_edges.csv",
    )
    bulk_load_to_neptune(s3_path)
