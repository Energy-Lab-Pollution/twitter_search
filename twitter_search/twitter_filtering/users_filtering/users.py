"""
Script in charge of filtering users based on their location and content relevance.
"""

from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from transformers import pipeline
from config_utils import util, constants


class UserFilter:
    def __init__(self, location, input_file, output_file):
        """
        Initialize UserFilter with a specific location.

        Args:
            location (str): The location to filter users from.
        """

        self.location = location
        self.relevant_labels = constants.RELEVANT_LABELS
        self.input_file = input_file
        self.output_file = output_file
        self.pipeline = pipeline(
            constants.HUGGINGFACE_PIPELINE, model=constants.HUGGINGFACE_MODEL
        )

    def load_and_preprocess_data(self):
        """Load and preprocess data from JSON file."""

        # data_dir = Path(__file__).parent.parent.parent.parent \
        # / "twitter_search/data/raw_data"
        # input_file = data_dir / f"{self.location}_users_test.json"
        try:
            self.users_list = util.load_json(self.input_file)
            self.total_user_dict = util.flatten_and_remove_empty(self.users_list)
        except Exception as e:
            print(f"Error loading data: {e}")
            self.total_user_dict = []

    def classify_content_relevance(self):
        """Classify content relevance for each user based on
        their name, bio, and tweets

        We use a pre-trained model from Hugging Face to classify
        """
        count = 0
        for user in self.total_user_dict:
            count += 1
            length = len(self.total_user_dict)
            print(f"{count} users done out of {length}")
            user["token"] = " ".join(
                [
                    user["username"],
                    user["description"] if user["description"] is not None else "",
                    user["location"] if user["location"] is not None else "",
                    " ".join(user["tweets"]) if user["tweets"] is not None else "",
                ]
            )
            try:
                classification = self.pipeline(
                    user["token"], candidate_labels=self.relevant_labels
                )
                relevant_labels_predicted = [
                    label
                    for label, score in zip(
                        classification["labels"], classification["scores"]
                    )
                    if score > 0.5
                ]

                user["content_is_relevant"] = bool(relevant_labels_predicted)
                user["content_labels"] = relevant_labels_predicted
            except Exception as e:
                print(
                    f"Error classifying content relevance for user \
                      {user['username']}: {e}"
                )
                user["content_is_relevant"] = False
                user["content_labels"] = []

    def determine_location_relevance(self):
        """Determine the relevance of
        user location"""

        for user in self.total_user_dict:

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
                                Point(user["geo_location"][1], user["geo_location"][0])
                            ]
                        },
                        crs="EPSG:4326",
                    )

                    shapefile_path = (
                        Path(__file__).parent.parent
                        / "utils/shape_files/geoBoundaries-IND-ADM1-all/geoBoundaries-IND-ADM1_simplified.shp"
                    )

                    shapefile = gpd.read_file(shapefile_path)
                    joined_data = gpd.sjoin(
                        user_location, shapefile, how="left", op="within"
                    )

                    print(joined_data.head())
                    try:
                        subnational = joined_data["shapeName"].iloc[0].lower()
                    except:
                        subnational = None
                    print(subnational, "subnational")
                    desired_locations = constants.STATE_CAPITALS.get(self.location, [])
                    print("desired locations", desired_locations)
                    user["location_relevance"] = subnational in desired_locations
                else:
                    user["location_relevance"] = False

    def remove_users(self):

        self.filtered_user = []
        for user in self.total_user_dict:
            if (
                user["location_relevance"] is True
                and user["content_is_relevant"] is True
            ):
                self.filtered_user.append(user)

    def store_users(self):

        try:
            util.json_maker(self.output_file, self.filtered_user)
        except Exception as e:
            print(f"Error storing filtered users: {e}")

    def run_filtering(self):
        """
        Run the filtering process for a specific location.

        Args:
            location (str): The location to filter users from.
        """
        try:
            self.load_and_preprocess_data()
            print("data preprocessed, step 2 done yay \n")
            self.classify_content_relevance()
            print(
                """users classified based on name, bio, \n
                and their tweets, step 3 done \n"""
            )
            self.determine_location_relevance()
            print(f"relevant users for {self.location} tagged step 4 done \n")
            self.remove_users()
            print("non-relevant users removed, step 5 completed \n")
            self.store_users()
            print("Filtered users stored successfully.")

        except Exception as e:
            print(f"An error occurred during filtering: {e}")
