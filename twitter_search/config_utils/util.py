"""
Util script with different functions used throughout the project
"""

import json
import os
import re
from datetime import datetime

import boto3
import botocore
import tweepy

# Local imports
from config_utils import config
from config_utils.cities import ALIAS_DICT
from config_utils.constants import REGION_NAME


LIST_FIELDS = ["id", "name", "description"]
USER_FIELDS = [
    "created_at",
    "description",
    "entities",
    "id",
    "location",
    "most_recent_tweet_id",
    "name",
    "pinned_tweet_id",
    "profile_image_url",
    "protected",
    "public_metrics",
    "url",
    "username",
]


def strtobool(val):
    """Convert a string representation of truth to true (1) or false (0).
    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "True", "on", "1"):
        return True
    elif val in ("n", "no", "f", "false", "False", "off", "0"):
        return False
    else:
        raise ValueError("invalid truth value %r" % (val,))


def list_filter_keywords(all_lists, location):
    filtered_lists = set()
    keywords = {
        f"{keyword.lower()} {location.lower()}"
        for keyword in [
            "journalists",
            "journalism",
            "news",
            "politics",
            "ngo",
            "pollution",
            "air",
            "health",
            "swatchh",
            "clean",
            "climate",
            "energy",
            "environment",
            "activist",
        ]
    } | {
        keyword.lower()
        for keyword in [
            "journalists",
            "journalism",
            "news",
            "politics",
            "ngo",
            "pollution",
            "air",
            "health",
            "swatchh",
            "clean",
            "climate",
            "energy",
            "environment",
            "activist",
        ]
    }

    for lst in all_lists:
        list_name_lower = lst["name"].lower()
        list_description_lower = lst.get(
            "description", ""
        ).lower()  # Handle missing descriptions

        # Check if any keyword intersects with the lowercase name/description
        if any(
            keyword in list_name_lower or keyword in list_description_lower
            for keyword in keywords
        ):
            filtered_lists.add(lst["list_id"])

    return list(filtered_lists)


# JSON manipulation util functions


def load_json(path):
    """
    Reads a JSON file and returns the data
    """
    try:
        with open(path, "r") as f:
            existing_data = json.load(f)
    except FileNotFoundError:
        existing_data = []

    return existing_data


def remove_duplicate_records(records):
    """
    Removes duplicate tweet/user dictionaries
    from a list of dictionaries.

    Parameters
    ----------
    records : list
        List of dictionaries

    Returns
    -------
    list
        List of dictionaries with duplicates removed
    """
    unique_records = []
    seen_records = set()

    for record in records:
        if "user_date_id" in record:
            record_id = record["user_date_id"]

        elif "tweet_id" in record:
            record_id = record["tweet_id"]

        elif "user_id" in record:
            record_id = record["user_id"]

        else:
            continue

        if record_id not in seen_records:
            unique_records.append(record)
            seen_records.add(record_id)

    return unique_records


def flatten_and_remove_empty(input_list):
    """
    Flatten a list of lists into a single list and remove any empty lists within it.

    Args:
        input_list (list): The list of lists to be flattened and cleaned.

    Returns:
        list: The flattened list with empty lists removed.
    """
    new_list = []
    for item in input_list:
        if isinstance(item, list):
            subitems = [subitem for subitem in item]
            new_list.extend(subitems)
        else:
            new_list.append(item)

    return new_list


# ================== GeoLocation ======================


def geocode_address(address, geolocator):
    """
    Geocodes an address using the geopy library
    """
    try:
        location = geolocator.geocode(address, timeout=GEOCODE_TIMEOUT)

        if location:
            return location.latitude, location.longitude
        else:
            print(f"Address '{address}' could not be geocoded.")
            return None, None

    except GeocoderTimedOut:
        print(f"Geocoding service timed out for address: '{address}'")
        return None, None

    except GeocoderServiceError as e:
        print(f"Geocoding service error: {e}")
        return None, None


# ================= Tweepy utils =========================


def client_creator():
    """
    Creates a wrapper for the Twitter API client.
    """
    consumer_key = config.consumer_key
    consumer_secret = config.consumer_secret
    access_token = config.access_token
    access_token_secret = config.access_token_secret
    bearer_token = config.bearer_token

    return tweepy.Client(
        bearer_token=bearer_token,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
        wait_on_rate_limit=True,
    )


def api_v1_creator():
    """
    Creates a tweepy.API wrapper for Twitter v1.1 endpoints (e.g. followers/list).
    """
    consumer_key = config.consumer_key
    consumer_secret = config.consumer_secret
    access_token = config.access_token
    access_token_secret = config.access_token_secret

    # 1) Build the OAuth1 handler
    auth = tweepy.OAuth1UserHandler(
        consumer_key, consumer_secret, access_token, access_token_secret
    )

    # 2) Create and return the API object, with rate-limit handling turned on
    return tweepy.API(
        auth,
        wait_on_rate_limit=True,
        retry_count=3,  # auto-retry transient network errors
        retry_delay=5,
        retry_errors={401, 404, 500, 502, 503, 504},
    )


def tweet_dictmaker(tweet_list):
    """
    This function takes a list of tweet objects and
    transforms it into a list of dictionaries

    Parameters
    ----------
    tweet_list : list
        List of tweet objects

    Returns
    -------
    dict_list: list
        List of dictionaries with tweet data
    """
    dict_list = []
    for tweet in tweet_list:
        values = {
            "tweet_id": tweet.id,
            "text": tweet.text,
            "author_id": tweet.author_id,
            "created_at": tweet.created_at.isoformat(),  # Convert datetime to ISO format string
            "conversation_id": tweet.conversation_id,
            "geo": tweet.geo,
            "lang": tweet.lang,
            "possibly_sensitive": tweet.possibly_sensitive,
            "reply_settings": tweet.reply_settings,
            "source": tweet.source,
            "attachments": tweet.attachments,
            "context_annotations": tweet.context_annotations,
            "entities": tweet.entities,
            "public_metrics": tweet.public_metrics,
            "non_public_metrics": tweet.non_public_metrics,
            "organic_metrics": tweet.organic_metrics,
            "promoted_metrics": tweet.promoted_metrics,
            "withheld": tweet.withheld,
            "in_reply_to_user_id": tweet.in_reply_to_user_id,
        }
        dict_list.append(values)
    return dict_list


def user_dictmaker(user_list):
    """
    This function takes a list of user objects and
    transformis it into a list of dictionaries

    Parameters
    ----------
    user_list : list
        List of user objects

    Returns
    -------
    dict_list: list
        List of dictionaries with user data
    """
    dict_list = []
    for user in user_list:
        values = {
            "user_id": user["id"],
            "username": user["username"],
            "description": user["description"],
            "location": user["location"],
            "name": user["name"],
            "url": user["url"],
            "tweets": [],
            "geo_code": [],
        }
        values.update(user["public_metrics"])
        dict_list.append(values)
    return dict_list


def list_dictmaker(incoming_datastruct):
    """
    - Function description -

    Parameters
    ----------
    incoming_datastruct : _type_
        _description_

    Returns
    -------
    dict_list: list
    """
    dict_list = []
    for userid, lsts in incoming_datastruct.items():
        for lst in lsts:
            values = {
                "user_id": userid,
                "list_id": lst["id"],
                "name": lst["name"],
                "created_at": lst["created_at"],
                "description": lst["description"],
                "follower_count": lst["follower_count"],
                "member_count": lst["member_count"],
                "private": lst["private"],
                "owner_id": lst["owner_id"],
            }
            dict_list.append(values)
    return dict_list


# ================== Location checking util function ===========


def check_location(raw_location, target_location):
    """
    Uses regex to see if the raw location matches
    the target location
    """
    if target_location in ["guatemala"]:
        target_locations = []
    else:
        target_locations = [target_location]

    # alias is the key, target loc is the value
    for alias, value in ALIAS_DICT.items():
        if value == target_location:
            target_locations.append(alias)

    if isinstance(raw_location, str):
        raw_location = raw_location.lower().strip()
        location_regex = re.findall(r"\w+", raw_location)

        if location_regex:
            for target_location in target_locations:
                if target_location in location_regex:
                    return True
                elif target_location in raw_location:
                    return True
            else:
                return False
        else:
            return False
    else:
        return False


# ============================== JSON Creators =========================


def json_maker(file_path, data_to_append):
    """
    Create a JSON file with the data provided, the
    JSON is saved in the file path provided.

    Parameters
    ----------
    file_path : str
        The path where the JSON file will be saved.

    data_to_append : dict
        The data to be saved in the JSON file.
    """
    try:
        with open(file_path, "r") as f:
            existing_data = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        existing_data = []

    # Extend the new data to the existing list
    existing_data.extend(data_to_append)

    # Keep only unique dictionaries in the list
    existing_data = set(json.dumps(d, sort_keys=True) for d in existing_data)
    existing_data = [json.loads(d) for d in existing_data]

    # Check if the file path exists
    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))
        print("Created directory:", os.path.dirname(file_path))

    # Write the updated list of dictionaries back to the file
    with open(file_path, "w") as f:
        json.dump(existing_data, f, indent=1)


def network_json_maker(file_path, data_to_append):
    """
    Create a JSON file with the data provided, the
    JSON is saved in the file path provided.

    Parameters
    ----------
    file_path : str
        The path where the JSON file will be saved.

    data_to_append : dict
        The data to be saved in the JSON file.
    """
    try:
        with open(file_path, "r") as f:
            existing_data = json.load(f)
    except FileNotFoundError:
        existing_data = []

    # Extend new data to existing data
    existing_data.extend(data_to_append)

    # Check if the file path exists
    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))
        print("Created directory:", os.path.dirname(file_path))

    # Write the updated list of dictionaries back to the file
    with open(file_path, "w") as f:
        json.dump(existing_data, f, indent=1)


# Date conversion
def convert_to_iso_format(date_string):
    """
    Converts a date string in the format "Fri Dec 06 18:09:05 +0000 2024"
    to isoformat

    Args:
        date_string: The input date string.

    Returns:
        The date string in the "yyyy-mm-dd" format.
    """
    try:
        date_obj = datetime.strptime(date_string, "%a %b %d %H:%M:%S %z %Y")
        return date_obj.isoformat()
    except ValueError:
        print(f"Invalid date format: {date_string}")
        return None


# ========================== AWS Utils ================================


def upload_to_s3(local_filename, s3_filename, bucket_name):
    """
    Uploads a given file to S3
    """
    s3_client = boto3.client("s3", region_name=REGION_NAME)
    try:
        s3_client.upload_file(
            Filename=local_filename, Bucket=bucket_name, Key=s3_filename
        )

    except botocore.exceptions.ClientError:
        print("Upload unsuccessful")
