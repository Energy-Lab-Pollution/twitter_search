"""
This script fetches the lists associated with the filtered users
"""

import time

import tweepy
import tweepy.errors
from config_utils import util
from config_utils.constants import (
    COUNT_THRESHOLD,
    MAX_RESULTS_LISTS,
    SLEEP_TIME,
)


class ListGetter:
    """
    This class is in charge of parsing all of the lists of the twitter users
    """

    def __init__(self, location, input_file, output_file):
        self.location = location
        self.input_file = input_file
        self.output_file = output_file
        self.MAX_RESULTS = MAX_RESULTS_LISTS
        self.COUNT_THRESHOLD = COUNT_THRESHOLD
        self.SLEEP_TIME = SLEEP_TIME
        self.client = util.client_creator()

    def get_list_membership(self, user):
        """
        Uses Twitter's API to get a users' lists
        """

        success = False
        while not success:
            try:
                response_user_list = self.client.get_list_memberships(
                    id=user["user_id"],
                    list_fields=util.LIST_FIELDS,
                    max_results=self.MAX_RESULTS,
                )
                success = True

            except tweepy.errors.TooManyRequests as error:
                print(f"{error} - sleeping...")
                time.sleep(self.SLEEP_TIME)

            except Exception as error:
                print(f"Unknown error, skipping: {error}")
                response_user_list = None
                break

        return response_user_list

    def get_lists_from_users(self, users_list):
        """
        Get lists from the current set of users.

        Parameters
        ----------
        client : tweepy.Client
            An authenticated Twitter API client.
        users_list : _type_
            _description_
        output_file : _type_
            _description_
        k : _type_, optional
            _description_, by default None
        """
        count = 0
        print(users_list)
        for user in users_list:

            if not isinstance(user, dict):
                continue

            response_user_list = self.get_list_membership(user)

            if response_user_list.data is None:
                print(f"No lists found for {user['user_id']}")
                continue

            only_lists = response_user_list.data
            print(
                f"there are {len(only_lists)} lists associated\
                   with {user['user_id']}"
            )
            if response_user_list.data is None:
                print("No lists found")
                continue

            list_entries = util.list_dictmaker({user["user_id"]: only_lists})
            util.json_maker(self.output_file, list_entries)
            count += 1
            if count > self.COUNT_THRESHOLD:
                print("You have to wait for 1 min")
                time.sleep(self.SLEEP_TIME)

    def load_users(self):
        """
        Loads json file with users data
        """
        users_list_raw = util.load_json(self.input_file)
        user_list = util.flatten_and_remove_empty(users_list_raw)

        return user_list

    def get_lists(self):
        """
        Reads lists of users from a JSON file, parses them
        and returns them.
        """
        try:
            user_list = self.load_users()
            print("Now obtaining lists that the users are a part of")
            print(f"We have {len(user_list)} users to get lists from")
            self.get_lists_from_users(user_list)

        except Exception as e:
            print(f"An error occurred: {e}")
