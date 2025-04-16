import json
import csv
import os
import boto3
import requests

# Configuration - Update these constants with your actual values
S3_BUCKET = "your-s3-bucket-name"
RETWEET_S3_PREFIX = "neptune/retweets/"
FOLLOW_S3_PREFIX = "neptune/follows/"
NEPTUNE_LOADER_ENDPOINT = "https://<your-neptune-endpoint>:8182/loader"
IAM_ROLE_ARN = "arn:aws:iam::<your-account-id>:role/YourNeptuneLoadRole"
AWS_REGION = "us-east-1"

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
            vfile, fieldnames=["~id", "~label", "username", "followers", "location"]
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
    payload = {
        "source": f"s3://{S3_BUCKET}/{s3_prefix}",
        "format": "csv",
        "iamRoleArn": IAM_ROLE_ARN,
        "region": AWS_REGION,
        "failOnError": "FALSE",
        "parallelism": "HIGH",
    }
    response = requests.post(NEPTUNE_LOADER_ENDPOINT, json=payload)
    print("Bulk load response:", response.status_code, response.json())


if __name__ == "__main__":
    # Example usage:
    # Convert and load retweets
    convert_json_to_csv(
        json_path="data/networks/kolkata/retweet_interactions.json",
        vertices_csv_path="outputs/kolkata_vertices.csv",
        edges_csv_path="outputs/kolkata_retweet_edges.csv",
        graph_type="retweet",
        location="kolkata",
    )
    upload_to_s3(
        "outputs/kolkata_vertices.csv", f"{RETWEET_S3_PREFIX}kolkata_vertices.csv"
    )
    upload_to_s3(
        "outputs/kolkata_retweet_edges.csv",
        f"{RETWEET_S3_PREFIX}kolkata_retweet_edges.csv",
    )
    bulk_load_to_neptune(RETWEET_S3_PREFIX)

    # Convert and load follows
    convert_json_to_csv(
        json_path="data/networks/kolkata/follower_interactions.json",
        vertices_csv_path="outputs/kolkata_vertices.csv",  # reuse or separate as needed
        edges_csv_path="outputs/kolkata_follow_edges.csv",
        graph_type="follow",
        location="kolkata",
    )
    upload_to_s3(
        "outputs/kolkata_vertices.csv", f"{FOLLOW_S3_PREFIX}kolkata_vertices.csv"
    )
    upload_to_s3(
        "outputs/kolkata_follow_edges.csv",
        f"{FOLLOW_S3_PREFIX}kolkata_follow_edges.csv",
    )
    bulk_load_to_neptune(FOLLOW_S3_PREFIX)
