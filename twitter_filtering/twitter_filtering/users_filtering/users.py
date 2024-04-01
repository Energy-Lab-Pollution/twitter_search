from pathlib import Path
import geopandas as gpd
from shapely.geometry import Point
from transformers import pipeline
from twitter_search.config_utils import util
from twitter_search.config_utils import constants

class UserFilter:
    def __init__(self, location):
        self.location = location
        self.relevant_labels = constants.RELEVANT_LABELS

    def load_and_preprocess_data(self):
        data_dir = Path(__file__).parent.parent.parent.parent / "twitter_search/data/raw_data"
        input_file = data_dir / f"{self.location}_users.json"
        try:
            users_list = util.load_json(input_file)
            self.total_user_dict = util.flatten_and_remove_empty(users_list)
        except Exception as e:
            print(f"Error loading data: {e}")
            self.total_user_dict = []

    def classify_content_relevance(self):
        pipe = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
        for user in self.total_user_dict:
            user['token'] = ' '.join([user['username'], user['user_description'], user['user_location'], ' '.join(user['tweets'])])
            try:
                classification = pipe(user['token'], candidate_labels=self.relevant_labels)
                relevant_labels_predicted = [label for label, score in zip(classification["labels"], classification["scores"]) if score > 0.5]
                user['content_is_relevant'] = bool(relevant_labels_predicted)
                user['content_labels'] = relevant_labels_predicted
            except Exception as e:
                print(f"Error classifying content relevance for user {user['username']}: {e}")
                user['content_is_relevant'] = False
                user['content_labels'] = []

    def determine_location_relevance(self):
        for user in self.total_user_dict:
            user_location = gpd.GeoDataFrame({'geometry': [Point(user['geo_location'][1], user['geo_location'][0])]}, crs='EPSG:4326')
            shapefile = gpd.read_file('/Users/praveenchandardevarajan/Downloads/geoBoundaries-IND-ADM1-all/geoBoundaries-IND-ADM1_simplified.shp')
            joined_data = gpd.sjoin(user_location, shapefile, how='left', op='within')
            subnational = joined_data['shapeName'].iloc[0].lower()
            desired_locations = constants.STATE_CAPITALS.get(self.location, [])
            user['location_relevance'] = subnational in desired_locations

    def store_users(self):
        output_dir = Path(__file__).parent.parent.parent / "data/raw_data"
        output_file = output_dir / f"{self.location}_users_filtered.json"
        try:
            util.json_maker(output_file, self.total_users_dict)
        except Exception as e:
            print(f"Error storing filtered users: {e}")

def run_filtering(location):
    try:
        user_filter = UserFilter(location)
        user_filter.load_and_preprocess_data()
        user_filter.classify_content_relevance()
        user_filter.determine_location_relevance()
        user_filter.store_users()
        print("Filtering completed successfully.")
    except Exception as e:
        print(f"An error occurred during filtering: {e}")

