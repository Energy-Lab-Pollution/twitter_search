"""
Script in charge of filtering users based on their location and content relevance.
"""

import concurrent.futures
import json
import os
from pathlib import Path

import geopandas as gpd
import torch
from config_utils import constants, util
from shapely.geometry import Point
from tqdm import tqdm
from transformers import pipeline


class UserFilter:
    SCORE_THRESHOLD = constants.SCORE_THRESHOLD
    RELEVANT_LABELS = constants.RELEVANT_LABELS
    STATE_CAPITALS = constants.STATE_CAPITALS
    NUM_WORKERS = constants.NUM_WORKERS
    BATCH_SIZE = constants.BATCH_SIZE

    def __init__(self, location, input_file, output_file):
        """
        Initialize UserFilter with a specific location.

        Args:
            location (str): The location to filter users from.
        """

        self.location = location
        self.input_file = input_file
        self.output_file = output_file

        # Setting up NLP classifier
        device = 0 if torch.cuda.is_available() else -1
        self.classifier = pipeline(
            constants.HUGGINGFACE_PIPELINE,
            model=constants.HUGGINGFACE_MODEL,
            device=device,
        )
        # TODO, based on location, select the appropriate shape file.
        self.shapefile_path = (
            Path(__file__).parent.parent
            / "utils/shape_files/geoBoundaries-IND-ADM1-all/geoBoundaries-IND-ADM1_simplified.shp"
        )

    def load_and_preprocess_data(self):
        """
        Load and preprocess data from JSON file.

        Removes duplicate records, etc.
        """

        # Read users, flatten list if necessary and remove duplicate recs
        self.users_list = util.load_json(self.input_file)
        self.total_user_dict = util.flatten_and_remove_empty(self.users_list)
        self.total_user_dict = util.remove_duplicate_records(self.total_user_dict)

        self.get_already_classified_users()

        print(f"Already classified {len(self.classified_users)} users")

        print(f"{len(self.unclassified_users)} users to classify")

    def get_already_classified_users(self):
        """
        Checks if the output JSON file still exists and
        gets the already classified users

        We then get a list of current _unclassified_ users
        """
        self.unclassified_users = []
        self.classified_users = []
        self.classified_users_ids = []

        if os.path.exists(self.output_file):
            classified_users_json = util.load_json(self.output_file)

            for classified_user in classified_users_json:
                if isinstance(classified_user, dict):
                    user_id = classified_user["user_id"]
                    self.classified_users_ids.append(user_id)
                    self.classified_users.append(classified_user)
                else:
                    continue

            for user in self.total_user_dict:
                if user["user_id"] in self.classified_users_ids:
                    continue
                else:
                    self.unclassified_users.append(user)

        else:
            print("No previously classified users")
            self.unclassified_users = self.total_user_dict.copy()

    @staticmethod
    def create_token(user):
        """
        This function will create the token to
        classify the user using the HF model
        """

        token = " ".join(
            [
                (user["description"] if user["description"] is not None else ""),
                (" ".join(user["tweets"]) if user["tweets"] is not None else ""),
            ]
        )

        return token

    def add_token_field(self, user):
        """
        Uses the 'create_token' function to add a new field
        """
        user["token"] = self.create_token(user)
        return user

    def classify_single_user(self, user):
        """
        This function classifies a single user
        """

        try:
            classification = self.classifier(
                user["token"], candidate_labels=self.RELEVANT_LABELS
            )

            max_score = max(classification["scores"])

            relevant_labels = [
                label
                for label, score in zip(
                    classification["labels"], classification["scores"]
                )
                if score == max_score
            ]

            user["content_is_relevant"] = (
                False if relevant_labels[0] == "other" else True
            )
            user["content_labels"] = relevant_labels[0]

        except Exception as error:
            print(f"Error classifying user: {error}")
            user["content_is_relevant"] = False
            user["content_labels"] = []

        return user

    def classify_users_in_batches(self, users, batch_size):
        """
        Batches the users for later processing
        """

        tokens = [user["token"] for user in users]
        results = []

        for index in tqdm(range(0, len(tokens), batch_size)):
            batch_tokens = tokens[index : index + batch_size]
            try:
                classifications = self.classifier(
                    batch_tokens,
                    candidate_labels=self.RELEVANT_LABELS,
                    batch_size=batch_size,
                    truncation=True,
                )
            except Exception as error:
                print(f"Error during batch classification {error}")
                for user in users[index : index + batch_size]:
                    user["content_is_relevant"] = False
                    user["content_labels"] = []
                continue

            for user, classification in zip(
                users[index : index + batch_size], classifications
            ):
                try:
                    max_score = max(classification["scores"])
                    relevant_labels = [
                        label
                        for label, score in zip(
                            classification["labels"], classification["scores"]
                        )
                        if score == max_score
                    ]

                    user["content_is_relevant"] = (
                        False if relevant_labels[0] == "other" else True
                    )
                    user["content_labels"] = relevant_labels[0]
                except Exception as e:
                    print(f"Error processing classification for a user: {e}")
                    user["content_is_relevant"] = False
                    user["content_labels"] = []

                results.append(user)

        return results

    def classify_users_concurrently(self, users):
        """
        Classifies all users using threads
        """
        results = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.NUM_WORKERS
        ) as executor:
            futures = [
                executor.submit(self.classify_single_user, user) for user in users
            ]

            for future in tqdm(
                concurrent.futures.as_completed(futures), total=len(futures)
            ):
                results.append(future.result())

        return results

    def classify_content_relevance(self):
        """Classify content relevance for each user based on
        their name, bio, and tweets

        We use a pre-trained model from Hugging Face to classify
        all the users concurrently
        """
        print(f"Classifying {len(self.unclassified_users)}")
        self.unclassified_users = list(
            map(self.add_token_field, self.unclassified_users)
        )

        self.unclassified_users = self.classify_users_in_batches(
            self.unclassified_users, batch_size=self.BATCH_SIZE
        )

    def determine_location_relevance(self):
        """Determine the relevance of
        user location"""

        for user in self.unclassified_users:
            if "geo_location" not in user:
                raise ValueError(
                    f"User geo_location not found, present fields are: {user.keys()}"
                )
            else:
                if (
                    user["geo_location"] is not None
                    and None not in user["geo_location"]
                ):
                    # Assuming user['geo_location'] is always a list
                    # with two elements (latitude and longitude)
                    # latitude, longitude = user['geo_location']
                    user_location = gpd.GeoDataFrame(
                        {
                            "geometry": [
                                Point(
                                    user["geo_location"][1],
                                    user["geo_location"][0],
                                )
                            ]
                        },
                        crs="EPSG:4326",
                    )

                    shapefile = gpd.read_file(self.shapefile_path)
                    joined_data = gpd.sjoin(
                        user_location, shapefile, how="left", predicate="within"
                    )
                    try:
                        subnational = joined_data["shapeName"].iloc[0]
                        if isinstance(subnational, float):
                            print(
                                f"Subnational is float, skipping user: {user['username']}"
                            )
                            subnational = None
                            user["location_relevance"] = False
                            continue
                        else:
                            subnational = subnational.lower()
                    except Exception as e:
                        print(f"Error determining subnational location: {e}")
                        subnational = None
                    print(subnational, "subnational")
                    desired_locations = self.STATE_CAPITALS.get(self.location, [])
                    print("desired locations", desired_locations)
                    user["location_relevance"] = subnational in desired_locations
                else:
                    user["location_relevance"] = False

    def remove_users(self):
        """
        Removes users that are not relevant based on their location and content.
        """

        self.filtered_users = []
        for user in self.all_users:
            # if user["content_is_relevant"] is True:
            self.filtered_users.append(user)

        print(f"Filtered {len(self.filtered_users)} relevant users")

    def store_users(self):
        """
        Stores users on a JSON file; checks if the JSON exists. If it does, read it and
        see if the user is already there. If it is, skip it.
        """
        try:
            util.json_maker(self.output_file, self.all_users)
        except Exception as e:
            print(f"Error storing filtered users: {e}")

    def rewrite_json_file(self):
        """
        Overwrites the json file of the users
        """
        # Write the updated list of dictionaries back to the file
        with open(self.output_file, "w") as file:
            json.dump(self.all_users, file, indent=1)

    def reclassify_all_users(self):
        """
        Re-classifies all the users

        This method should be used when one wants to re-apply the
        classification to all available users
        """
        try:
            print("Re-classifying all users")
            self.load_and_preprocess_data()
            self.unclassified_users = self.total_user_dict.copy()
            self.unclassified_users = util.remove_duplicate_records(
                self.unclassified_users
            )
            self.classify_content_relevance()
            # Paste both classified and unclassified users
            self.all_users = []
            self.all_users.extend(self.unclassified_users)
            print("Reclassified all users")
            self.remove_users()
            self.rewrite_json_file()
            print("Filtered users stored successfully.")

        except FileNotFoundError as e:
            print(f"An error occurred during filtering: {e}")

    def run_filtering(self):
        """
        Run the filtering process for a specific location.

        Args:
            location (str): The location to filter users from.
        """
        try:
            self.load_and_preprocess_data()
            print("data preprocessed, step 2 done")
            self.classify_content_relevance()
            print(
                """users classified based on name, bio, and their tweets,
                    step 3 done"""
            )

            # if self.location in self.STATE_CAPITALS:
            #     self.determine_location_relevance()
            #     print(f"relevant users for {self.location} tagged step 4 done")
            # else:
            #     print(f"Location {self.location} not found in STATE_CAPITALS")

            # Paste both classified and unclassified users
            self.all_users = []
            self.all_users.extend(self.classified_users)
            self.all_users.extend(self.unclassified_users)

            print("Extended both classified and unclassified users")

            self.remove_users()
            self.store_users()
            print("Filtered users stored successfully.")

        except FileNotFoundError as e:
            print(f"An error occurred during filtering: {e}")
