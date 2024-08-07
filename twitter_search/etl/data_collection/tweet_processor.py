"""
This script processes and parses the tweets corresponding to each user
"""

# Local imports
from config_utils import constants, util
from geopy.geocoders import Nominatim


class TweetProcessor:
    def __init__(self, location, account_type, input_file_tuple, output_file):
        self.location = location
        self.account_type = account_type
        self.geolocator = Nominatim(user_agent="EnergyLab")
        self.input_file_tweets, self.input_file_users = input_file_tuple
        self.output_file = output_file
        self.STATE_CAPITALS = constants.STATE_CAPITALS

    def get_coordinates(self, location):
        """
        Get the latitude and longitude coordinates of a location.
        """
        if location is None:
            return (None, None)
            # Geocode the location using Geopy
        else:
            lat, lng = util.geocode_address(location, self.geolocator)
            return (lat, lng)

    def process_tweets_for_users(self):
        """
        Adds tweets to each user's dictionary.

        Args:
            data: Response data containing tweets and users.

        Returns:
            None
        """
        for tweet in self.tweet_list:
            # entity_list = []
            # print(tweet)
            # for annotation in tweet["context_annotations"]:
            #     domain_name = annotation["domain"]["name"].lower()
            #     print(domain_name)
            #     entity_name = annotation["entity"].get("name", "").lower()
            #     print(entity_name)
            #     relevant_categories = constants.category_dict.get(self.account_type,[])
            #     if domain_name in relevant_categories:
            #     entity_list.append({domain_name:entity_name})
            #     is_relevant = (domain_name in relevant_categories ) and \
            #                     (entity_name == constants.\
            #                      STATE_CAPITALS[self.location] or \
            #                         entity_name == self.location)

            author_id = tweet.get("author_id", None)
            if author_id:
                for user in self.user_list:
                    if user["user_id"] == author_id:
                        user["tweets"].append(tweet["text"])
                        # user["tweet_info"] = entity_list
                        # try:
                        #     if is_relevant:
                        #         user['detected_loc'] = self.location
                        #         user['detected_cat'] = "News"
                        # except:
                        #         user['detected_loc'] = ""
                        #         user['detected_cat'] = ""

    def geo_coder(self):
        """
        Runs the geocoding process for all users.
        """
        for user in self.user_list:
            user["geo_location"] = self.get_coordinates(user["location"])

    def store_users(self):
        """
        convert the user list to a json and store it.

        Args:
            None

        Returns:
            None
        """
        util.json_maker(self.output_file, self.user_list)
        print("Total number of users:", len(self.user_list))

    def run_processing(self):
        """
        Runs the entire process
        """
        tweet_list_raw = util.load_json(self.input_file_tweets)
        user_list_raw = util.load_json(self.input_file_users)
        self.tweet_list = util.flatten_and_remove_empty(tweet_list_raw)
        self.user_list = util.flatten_and_remove_empty(user_list_raw)
        self.process_tweets_for_users()
        self.geo_coder()
        self.store_users()
