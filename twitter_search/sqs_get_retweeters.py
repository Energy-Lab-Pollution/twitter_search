"""
Script to get a determined tweet's retweeters
"""
import asyncio
from datetime import datetime, timezone
import json
import time
from argparse import ArgumentParser

import boto3
import botocore
import twikit

from config_utils.constants import (
    FIFTEEN_MINUTES,
    NEPTUNE_S3_BUCKET,
    REGION_NAME,
    SQS_USER_RETWEETERS,    
    SQS_USER_TWEETS,
    TWIKIT_COOKIES_DICT,
)
from config_utils.util import (
    check_location,
    client_creator,
    convert_to_iso_format,
)

class TweetRetweeters:
    
    def __init__(self, location):
        self.location = location

    def parse_x_users(self, user_list):
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
        user_dicts = []
        for user in user_list:
            user_dict = {
                "user_id": user["id"],
                "username": user["username"],
                "description": user["description"],
                "profile_location": user["location"],
                "target_location": self.location,
                "verified": user["verified"],
                "created_at": user["created_at"],
                "processing_status": "pending",
            }
            for key, value in user["public_metrics"].items():
                user_dict[key] = value

            # TODO: Adding new attributes
            user_dict["category"] = "null"
            user_dict["treatment_arm"] = "null"
            # Followers and retweeters status
            user_dict["retweeter_status"] = "pending"
            user_dict["retweeter_last_processed"] = "null"
            user_dict["follower_status"] = "pending"
            user_dict["follower_last_processed"] = "null"
            user_dict["extracted_at"] = datetime.now(timezone.utc).isoformat()
            user_dict["last_updated"] = datetime.now(timezone.utc).isoformat()
            # See if location matches to add city
            location_match = check_location(user["location"], self.location)
            user_dict["city"] = self.location if location_match else None
            user_dicts.append(user_dict)

        return user_dicts

    def parse_twikit_users(self, users):
        """
        Parse retweeters (user objects) and put them
        into a list of dictionaries

        Args:
        ----------
            - tweets (list): list of twikit.User objects
        Returns:
        ----------
            - dict_list (list): list of dictionaries with users' info
        """
        users_list = []

        if users:
            for user in users:
                user_dict = {}
                user_dict["user_id"] = user.id
                user_dict["username"] = user.screen_name
                user_dict["description"] = user.description
                user_dict["profile_location"] = user.location
                user_dict["target_location"] = self.location
                user_dict["followers_count"] = user.followers_count
                user_dict["following_count"] = user.following_count
                user_dict["tweets_count"] = user.statuses_count
                user_dict["verified"] = user.verified
                user_dict["created_at"] = convert_to_iso_format(user.created_at)
                user_dict["category"] = "null"
                user_dict["treatment_arm"] = "null"
                user_dict["retweeter_status"] = "pending"
                user_dict["retweeter_last_processed"] = "null"
                user_dict["follower_status"] = "pending"
                user_dict["follower_last_processed"] = "null"
                user_dict["extracted_at"] = datetime.now(timezone.utc).isoformat()
                user_dict["last_updated"] = datetime.now(timezone.utc).isoformat()
                # See if location matches to add city
                location_match = check_location(user.location, self.location)
                user_dict["city"] = self.location if location_match else None
                users_list.append(user_dict)

        return users_list

    async def get_single_tweet_retweeters(self, tweet_id, num_retweeters, account_num):
        """
        For a particular tweet, get all the possible retweeters

        Args:
        ---------
            - tweet_id (str): String with tweet id
        Returns:
        ---------
            - retweeters_list (list): List with retweeters info
        """
        client = twikit.Client("en-US")
        client.load_cookies(TWIKIT_COOKIES_DICT[f"account_{account_num}"])
        
        retweeters_list = []
        more_retweeters_available = True
        self.retweeters_counter += 1
        attempt_number = 1

        # Maxed out retweeters threshold
        try:
            retweeters = await self.client.get_retweeters(
                tweet_id, count=num_retweeters
            )
            if retweeters:
                parsed_retweeters = self.parse_twikit_users(retweeters)
                retweeters_list.extend(parsed_retweeters)
        except twikit.errors.TooManyRequests:
            print("Retweeters: Too Many Requests")
            self.retweeters_maxed_out = True
            return None

        while more_retweeters_available:
            self.retweeters_counter += 1
            if self.retweeters_counter < self.TWIKIT_RETWEETERS_THRESHOLD:
                try:
                    if attempt_number == 1:
                        more_retweeters = await retweeters.next()
                    else:
                        more_retweeters = await more_retweeters.next()
                # Stop here if failure and return what you had so far
                except twikit.errors.TooManyRequests:
                    print("Retweeters: Too Many Requests")
                    print(f"Made {self.retweeters_counter} retweets requests")
                    self.retweeters_maxed_out = True
                    return retweeters_list
                except twikit.errors.BadRequest:
                    print("Retweeters: Bad Request")
                    return retweeters_list
                except twikit.errors.TwitterException as e:
                    print(f"Retweeters: Twitter Exception {e}")
                    return retweeters_list
                if more_retweeters:
                    more_parsed_retweeters = self.parse_twikit_users(
                        more_retweeters
                    )
                    retweeters_list.extend(more_parsed_retweeters)
                else:
                    more_retweeters_available = False
            else:
                print("Maxed out on retweeters threshold")
                print(f"Made {self.retweeters_counter} retweets requests")
                self.retweeters_maxed_out = True
                return retweeters_list

            attempt_number += 1

        return retweeters_list

    def x_get_single_tweet_retweeters(self, tweet_id, num_retweeters):
        """
        Pull up to 500 retweeters via v2, then parse.
        """
        retweeters_list = []
        next_token = None
        x_client = client_creator()

        while len(retweeters_list) < num_retweeters:
            response = x_client.get_retweeters(
                id=tweet_id,
                max_results=num_retweeters,
                pagination_token=next_token,
                user_fields=[
                    "id",
                    "username",
                    "description",
                    "location",
                    "public_metrics",
                    "verified",
                    "created_at",
                ],
            )

            retweeters = (
                [user.data for user in response.data] if response.data else []
            )
            if not retweeters:
                break
            retweeters_list.extend(retweeters)

            if len(retweeters) >= num_retweeters:
                break

            next_token = response.meta.get("next_token")
            if not next_token:
                break

        return self.parse_x_users(retweeters)

if __name__ == "__main__":
    parser = ArgumentParser(
        "Parameters to get users data to generate a network"
    )
    parser.add_argument(
        "--tweet_count", type=int, help="Number of tweets to get"
    )
    parser.add_argument(
        "--extraction_type",
        type=str,
        choices=["twikit", "X"],
        help="Choose how to get user's tweets",
    )
    parser.add_argument(
        "--account_num",
        type=int,
        help="Account number to use with twikit",
    )

    args = parser.parse_args()