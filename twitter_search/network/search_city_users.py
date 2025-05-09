"""
Script to search tweets and users from a particular location.
The users who match a certain criteria will be sent to a processing queue.
"""

import datetime
import json

import boto3
import twikit
from config_utils.constants import (
    EXPANSIONS,
    MAX_RESULTS,
    TWEET_FIELDS,
    TWIKIT_COOKIES_DIR,
    TWIKIT_COUNT,
    TWIKIT_TWEETS_THRESHOLD,
    USER_FIELDS,
)
from config_utils.util import (
    check_location,
    client_creator,
    convert_to_iso_format,
)


class CityUsers:
    def __init__(self, location):
        self.location = location
        self.sqs_client = boto3.client("sqs", region_name="")

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
            user_dict["category"] = None
            user_dict["treatment_arm"] = None
            user_dict["retweeter_status"] = "pending"
            user_dict["retweeter_last_processed"] = None
            user_dict["follower_status"] = "pending"
            user_dict["follower_last_processed"] = None
            user_dict["extracted_at"] = datetime.now().isoformat()
            user_dict["last_updated"] = datetime.now().isoformat()
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
                # TODO: Check difference between verified and is_blue_verified
                user_dict["verified"] = user.verified
                user_dict["created_at"] = convert_to_iso_format(user.created_at)
                # TODO: Adding new attributes
                user_dict["category"] = None
                user_dict["treatment_arm"] = None
                user_dict["retweeter_status"] = "pending"
                user_dict["retweeter_last_processed"] = None
                user_dict["follower_status"] = "pending"
                user_dict["follower_last_processed"] = None
                user_dict["extracted_at"] = datetime.now().isoformat()
                user_dict["last_updated"] = datetime.now().isoformat()
                # See if location matches to add city
                location_match = check_location(user.location, self.location)
                user_dict["city"] = self.location if location_match else None
                users_list.append(user_dict)

        return users_list

    async def _get_twikit_city_users(self):
        """
        Method used to search for tweets, with twikit,
        using a given query

        This method uses twikit's "await next" function
        to get more tweets with the given query. The corresponding
        users are then parsed from such tweets.
        """
        client = twikit.Client("en-US")
        client.load_cookies(TWIKIT_COOKIES_DIR)
        users_list = []

        tweets = await client.search_tweet(
            self.location, "Latest", count=TWIKIT_COUNT
        )
        # TODO: Add set operation to keep unique users only
        users_list = self.parse_twikit_users(tweets)

        more_tweets_available = True
        num_iter = 1

        next_tweets = await tweets.next()
        if next_tweets:
            next_users_list = self.parse_twikit_users(next_tweets)
            users_list.extend(next_users_list)
        else:
            more_tweets_available = False

        while more_tweets_available:
            next_tweets = await next_tweets.next()
            if next_tweets:
                next_users_list = self.parse_twikit_users(next_tweets)
                users_list.extend(next_users_list)

            else:
                more_tweets_available = False

            if num_iter % 5 == 0:
                print(f"Processed {num_iter} batches")

            # TODO: We may leave this entire process running
            if num_iter == TWIKIT_TWEETS_THRESHOLD:
                break

            num_iter += 1

        return users_list

    def _get_x_city_users(self):
        """
        Method used to search for tweets, with the X API,
        using a given query

        The corresponding users are then parsed from
        such tweets.
        """
        x_client = client_creator()
        result_count = 0
        next_token = None
        users_list = []
        tweets_list = []

        while result_count < MAX_RESULTS:
            print(f"Max results is: {result_count}")
            response = x_client.search_recent_tweets(
                query=self.location,
                max_results=MAX_RESULTS,
                next_token=next_token,
                expansions=EXPANSIONS,
                tweet_fields=TWEET_FIELDS,
                user_fields=USER_FIELDS,
            )
            if response.meta["result_count"] == 0:
                print("No more results found.")
                break
            result_count += response.meta["result_count"]
            tweets_list.extend(response.data)
            users_list.extend(response.includes["users"])
            users_list = self.parse_x_users(users_list)
            try:
                next_token = response.meta["next_token"]
            except Exception as err:
                print(err)
                next_token = None

            if next_token is None:
                break

        return users_list

    async def _get_city_users(self, extraction_type):
        """
        Searches for city users with either twikit or X, then sends them
        to the corresponding queue
        """
        if extraction_type == "twikit":
            users_list = await self._get_twikit_city_users()
        elif extraction_type == "x":
            users_list = self._get_x_city_users()

        # TODO: Upload user attributes to Neptune ?

        # TODO: Send users to queue
        queue_url = self.sqs_client.get_queue_url(QueueName="")["QueueUrl"]
        for user in users_list:
            if user["city"]:
                message = {
                    "user_id": str(user["user_id"]),
                    "location": self.location,
                }
                self.sqs_client.send_message(
                    QueueUrl=queue_url,
                    messageGroupId=self.location,
                    messageBody=json.dumps(message)
                )
