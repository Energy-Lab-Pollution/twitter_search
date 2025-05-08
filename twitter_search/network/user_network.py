"""
Script to pull tweets and retweeters from a particular user,
"""

import re
import time
from datetime import datetime

import tweepy
import twikit
from config_utils.cities import ALIAS_DICT
from config_utils.constants import (
    SIXTEEN_MINUTES,
    TWIKIT_COOKIES_DIR,
    TWIKIT_COUNT,
    TWIKIT_FOLLOWERS_THRESHOLD,
    TWIKIT_RETWEETERS_THRESHOLD,
    TWIKIT_TWEETS_THRESHOLD,
    X_FOLLOWERS_PAGE_SIZE,
    X_MAX_FOLLOWERS,
    X_MAX_RETWEETERS,
    X_MAX_USER_TWEETS,
    X_TWEETS_PAGE_SIZE,
)
from config_utils.util import (
    api_v1_creator,
    client_creator,
    convert_to_iso_format,
    network_json_maker,
)


class UserNetwork:
    TWIKIT_THRESHOLD = TWIKIT_TWEETS_THRESHOLD
    TWIKIT_FOLLOWERS_THRESHOLD = TWIKIT_FOLLOWERS_THRESHOLD
    TWIKIT_RETWEETERS_THRESHOLD = TWIKIT_RETWEETERS_THRESHOLD
    TWIKIT_COOKIES_DIR = TWIKIT_COOKIES_DIR
    TWIKIT_COUNT = TWIKIT_COUNT
    SLEEP_TIME = 2

    def __init__(self, output_file_path, location):
        # Twikit Client
        self.client = twikit.Client("en-US")
        self.client.load_cookies(self.TWIKIT_COOKIES_DIR)

        # X Tweepy Client
        self.x_client = client_creator()
        self.x_v1_client = api_v1_creator()

        self.location = location
        self.output_file_path = output_file_path

        # Setting retweeters counter at the top level
        self.retweeters_counter = 0

    @staticmethod
    def check_location(raw_location, target_location):
        """
        Uses regex to see if the raw location matches
        the target location
        """

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
            user_dict["processing_status"] = "pending"
            user_dict["extracted_at"] = datetime.now().isoformat()
            user_dict["last_processed"] = datetime.now().isoformat()
            user_dict["last_updated"] = datetime.now().isoformat()
            # See if location matches to add city
            location_match = self.check_location(
                user["location"], self.location
            )
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
                user_dict["processing_status"] = "pending"
                user_dict["extracted_at"] = datetime.now().isoformat()
                user_dict["last_processed"] = datetime.now().isoformat()
                user_dict["last_updated"] = datetime.now().isoformat()
                # See if location matches to add city
                location_match = self.check_location(
                    user.location, self.location
                )
                user_dict["city"] = self.location if location_match else None
                users_list.append(user_dict)

        return users_list

    @staticmethod
    def parse_twikit_tweets(tweets):
        """
        Given a set of tweets, we get a list of dictionaries
        with the tweets' and retweeters' information

        Args:
        ----------
            - tweets (list): list of twikit.Tweet objects
        Returns:
        ----------
            - dict_list (list): list of dictionaries with tweets info
        """
        dict_list = []
        if tweets:
            for tweet in tweets:
                tweet_dict = {}
                tweet_dict["tweet_id"] = tweet.id
                tweet_dict["tweet_text"] = tweet.text
                tweet_dict["created_at"] = convert_to_iso_format(
                    tweet.created_at
                )
                tweet_dict["retweet_count"] = tweet.retweet_count
                tweet_dict["favorite_count"] = tweet.favorite_count
                dict_list.append(tweet_dict)

        return dict_list

    @staticmethod
    def parse_x_tweets(tweets_list):
        """
        Normalize Tweepy v2 Tweet objects into dicts like your Twikit parser.
        """
        parsed_tweets = []
        for tweet in tweets_list:
            # t.created_at is a datetime
            created = (
                tweet.created_at.isoformat()
                if isinstance(tweet.created_at, datetime)
                else tweet.created_at
            )

            parsed_tweets.append(
                {
                    "tweet_id": tweet.id,
                    "tweet_text": tweet.text,
                    "created_at": created,
                    "retweet_count": tweet.public_metrics.get(
                        "retweet_count", 0
                    ),
                    "favorite_count": tweet.public_metrics.get("like_count", 0),
                }
            )

        return parsed_tweets

    async def get_single_tweet_retweeters(self, tweet_id):
        """
        For a particular tweet, get all the possible retweeters

        Args:
        ---------
            - tweet_id (str): String with tweet id
        Returns:
        ---------
            - retweeters_list (list): List with retweeters info
        """
        retweeters_list = []
        more_retweeters_available = True
        self.retweeters_counter += 1
        attempt_number = 1

        # Maxed out retweeters threshold
        if self.retweeters_counter == self.TWIKIT_RETWEETERS_THRESHOLD:
            print("Maxed out on retweeters threshold")
            self.retweeters_maxed_out = True
            return []

        try:
            retweeters = await self.client.get_retweeters(
                tweet_id, count=self.TWIKIT_COUNT
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

    async def twikit_add_retweeters(self, tweets_list):
        """
        For every tweet in a list of dictionaries, attempt to
        get all possible retweeters.

        Args:
        --------
            tweets_list (list): List of dictionaries

        Returns:
        --------
            new_tweets_list (list): List of dictionaries
        """
        new_tweets_list = []
        counter = 1
        # Variable to determine if no more requests on the retweeters
        self.retweeters_maxed_out = False

        for tweet_dict in tweets_list:
            # TODO: Upload tweet data to DynamoDB - store_data('tweet')
            # Only get retweeters if tweet is not a repost and retweet_count > 0
            if (not tweet_dict["tweet_text"].startswith("RT @")) and (
                tweet_dict["retweet_count"] > 0
            ):
                if not self.retweeters_maxed_out:
                    retweeters = await self.get_single_tweet_retweeters(
                        tweet_dict["tweet_id"]
                    )
                    # If retweeters, we add that field to the dict
                    if isinstance(retweeters, list):
                        # Check user location (?)
                        # TODO: Upload users data to DynamoDB - store_data('user')
                        # TODO: Send to SQS for network processing
                        tweet_dict["retweeters"] = retweeters
            if counter % 200 == 0:
                print(f"Processed {counter} tweets")
            counter += 1
            new_tweets_list.append(tweet_dict)

        return new_tweets_list

    def x_get_single_tweet_retweeters(self, tweet_id):
        """
        Pull up to 500 retweeters via v2, then parse.
        """
        retweeters_list = []
        next_token = None

        while len(retweeters_list) < X_MAX_RETWEETERS:
            response = self.x_client.get_retweeters(
                id=tweet_id,
                max_results=X_MAX_RETWEETERS,
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

            if len(retweeters) >= X_MAX_RETWEETERS:
                break

            next_token = response.meta.get("next_token")
            if not next_token:
                break

        return self.parse_x_users(retweeters)

    def x_add_retweeters(self, tweets_list):
        """
        For each tweet in tweets_list:
        - skip it if it's a pure retweet
        - if retweet_count > 0, fetch its retweeters
        - add a "retweeters" key to the tweet's dict
        """
        for tweet in tweets_list:
            # skip pure RTs
            refs = tweet.get("referenced_tweets") or []
            if any(ref.get("type") == "retweeted" for ref in refs):
                continue

            # only fetch if there actually are retweets
            if tweet.get("retweet_count", 0) > 0:
                tweet_id = tweet["tweet_id"]
                tweet["retweeters"] = self.x_get_single_tweet_retweeters(
                    tweet_id
                )

        return tweets_list

    async def twikit_get_user_tweets(self, user_id):
        """
        For a given user, we get as many of their tweets as possible
        and parse them into a list using twikit

        Args
        -------
            - client: Twikit client obj

        Returns:
        ---------
            - dict_list (list): list of dictionaries
        """
        # We need to get tweets first
        dict_list = []
        more_tweets_available = True
        num_iter = 0

        # Parse first set of tweets
        user_tweets = await self.client.get_user_tweets(
            user_id, "Tweets", count=self.TWIKIT_COUNT
        )
        tweets_list = self.parse_twikit_tweets(user_tweets)
        dict_list.extend(tweets_list)

        while more_tweets_available:
            num_iter += 1
            try:
                if num_iter == 1:
                    next_tweets = await user_tweets.next()
                else:
                    next_tweets = await next_tweets.next()
                if next_tweets:
                    # Parse next tweets
                    next_tweets_list = self.parse_twikit_tweets(next_tweets)
                    dict_list.extend(next_tweets_list)
                else:
                    more_tweets_available = False
            # If errored out on requests, just return what you already have
            except twikit.errors.TooManyRequests:
                print("Tweets: too many requests, stopping...")
                return dict_list
            if num_iter % 5 == 0:
                print(f"Processed {num_iter} user tweets batches")
                time.sleep(self.SLEEP_TIME)

            if num_iter >= self.TWIKIT_THRESHOLD:
                print("Tweets: Maxed out on requests")
                return dict_list

        return dict_list

    def x_get_user_tweets(self, user_id):
        """
        For a given user, we get as many of their tweets as possible
        and parse them into a list using x

        Args
        -------
            - client: Twikit client obj

        Returns:
        ---------
            - dict_list (list): list of dictionaries
        """
        user_tweets = []
        next_token = None
        while len(user_tweets) < X_MAX_USER_TWEETS:
            response = self.x_client.get_users_tweets(
                id=user_id,
                max_results=X_TWEETS_PAGE_SIZE,
                pagination_token=next_token,
                tweet_fields=["created_at", "public_metrics"],
            )

            page = response.date or []
            if not page:
                break

            user_tweets.extend(page)

            if (len(user_tweets) >= X_MAX_USER_TWEETS) or not (
                response.meta.get("next_token")
            ):
                break

        parsed_tweets = self.parse_x_tweets(user_tweets)

        return parsed_tweets

    async def twikit_get_followers(self, user_id):
        """
        Gets a given user's followers

        Args:
        ---------
            - user_id (str): User_id of user to look for
        Returns:
        ---------
            - tweet_list(list): List of dicts with followers info
        """
        followers_list = []
        max_retries = 3

        for attempt in range(1, max_retries + 1):
            try:
                # try to fetch
                followers = await self.client.get_user_followers(
                    user_id, count=self.TWIKIT_COUNT
                )
                break
            except twikit.errors.NotFound as error:
                if attempt < max_retries:
                    print(str(error))
                    print(
                        f"Followers: Not Found - retrying (attempt {attempt})"
                    )
                    time.sleep(90)
                else:
                    print(
                        f"Followers: Not Found after {max_retries} attempts - giving up"
                    )
                    return followers_list  # final failure
            except twikit.errors.TooManyRequests:
                print("Followers: Too Many Requests - stopping early")
                return followers_list
            except twikit.errors.BadRequest:
                print("Followers: Bad Request - stopping early")
                return followers_list
            except twikit.errors.TwitterException as e:
                print(f"Followers: Twitter Exception - {e}")
                return followers_list

        more_followers_available = True
        num_iter = 0

        parsed_followers = self.parse_twikit_users(followers)
        followers_list.extend(parsed_followers)

        # Keeping track of currently extracted users
        extracted_users = [
            parsed_follower["user_id"] for parsed_follower in parsed_followers
        ]

        while more_followers_available:
            num_iter += 1
            try:
                if num_iter == 1:
                    more_followers = await followers.next()
                else:
                    more_followers = await more_followers.next()
                if more_followers:
                    more_parsed_followers = self.parse_twikit_users(
                        more_followers
                    )
                    for parsed_follower in more_parsed_followers:
                        if parsed_follower["user_id"] not in extracted_users:
                            followers_list.append(parsed_follower)
                            extracted_users.append(parsed_follower["user_id"])
                        else:
                            continue
                else:
                    more_followers_available = False
            # Stop here and just return what you got
            except twikit.errors.TooManyRequests:
                print("Followers: too many requests, stopping...")
                return followers_list
            except twikit.errors.BadRequest:
                print("Followers: Bad Request")
                return followers_list
            except twikit.errors.NotFound:
                print("Followers: Not Found")
                return followers_list
            except twikit.errors.TwitterException as e:
                print(f"Followers: Twitter Exception {e}")
                return followers_list
            if num_iter % 5 == 0:
                print(f"Processed {num_iter} follower batches, sleeping...")
                time.sleep(self.SLEEP_TIME)
            if num_iter == self.TWIKIT_FOLLOWERS_THRESHOLD:
                print("Followers: maxed out number of requests")
                return followers_list

        # TODO: Upload users data to DynamoDB - store_data('user')
        # TODO: Send to SQS for network processing
        return followers_list

    def x_get_followers(self, user_id):
        """
        Pull up to 1 000 followers via v1.1, convert each to a dict
        shape that parse_x_users expects, then parse.
        """
        legacy_users = tweepy.Cursor(
            self.api_v1.followers, user_id=user_id, count=X_FOLLOWERS_PAGE_SIZE
        ).items(X_MAX_FOLLOWERS)

        # Convert to dicts
        normalized = []
        for legacy_user in legacy_users:
            normalized.append(
                {
                    "id": legacy_user.id_str,
                    "username": legacy_user.screen_name,
                    "description": legacy_user.description,
                    "location": legacy_user.location,
                    "created_at": legacy_user.created_at.isoformat(),
                    "verified": legacy_user.verified,
                    "public_metrics": {
                        "followers_count": legacy_user.followers_count,
                        "following_count": legacy_user.friends_count,
                        "tweet_count": legacy_user.statuses_count,
                        "listed_count": legacy_user.listed_count,
                    },
                }
            )

        return self.parse_x_users(normalized)

    async def run_twikit(self, user_dict):
        """
        Runs the pertinent functions by getting a user's retweeters and
        followers with twikit

        Args:
            - user_dict (dict): User dict
        """
        # TODO: processing_status is pending
        # extracted_at is null - take midpoint for when we got these root users
        # last_modified is null

        # First get tweets, without retweeters
        user_dict["processing_status"] = "in progress"
        print("Getting user tweets")
        user_tweets = await self.twikit_get_user_tweets(user_dict["user_id"])

        time.sleep(30)
        print("Getting user retweeters")
        user_tweets = await self.twikit_add_retweeters(user_tweets)
        user_dict["tweets"] = user_tweets

        print("Getting user followers...")
        time.sleep(45)
        followers = await self.twikit_get_followers(user_dict["user_id"])
        user_dict["followers"] = followers

        # Will put the extracted data into a list
        # Easier to extend future data
        user_dict_list = [user_dict]
        user_dict["last_processed"] = datetime.now().isoformat()
        user_dict["processing_status"] = "completed"

        network_json_maker(self.output_file_path, user_dict_list)
        print(f"Stored {user_dict['user_id']} data")

    def run_x(self, user_dict):
        """
        Runs the pertinent functions by getting a user's retweeters and
        followers

        Args:
            - user_dict (dict): User dict
        """
        # TODO: processing_status is pending
        # extracted_at is null - take midpoint for when we got these root users
        # last_modified is null
        # First get tweets, without retweeters
        user_dict["processing_status"] = "in progress"
        print("Getting user tweets")
        user_tweets = self.x_get_user_tweets(user_dict["user_id"])

        print("Getting user retweeters")
        user_tweets = self.x_add_retweeters(user_tweets)
        user_dict["tweets"] = user_tweets

        print("Getting user followers...")
        followers = self.x_get_followers(user_dict["user_id"])
        user_dict["followers"] = followers

        # Will put the extracted data into a list
        # Easier to extend future data
        user_dict_list = [user_dict]
        user_dict["last_processed"] = datetime.now().isoformat()
        user_dict["processing_status"] = "completed"

        network_json_maker(self.output_file_path, user_dict_list)
        print(f"Stored {user_dict["user_id"]} data")

    async def run(self, user_dict, extraction_type):
        """
        Gets the network data either using Twikit or X

        Args:
            - user_dict (dict): Dict with user data
            - extraction_type: str
        """
        if extraction_type == "x":
            self.run_x()
        else:
            # If file or twikit extraction method, use twikit
            await self.run_twikit(user_dict)
            time.sleep(SIXTEEN_MINUTES)
