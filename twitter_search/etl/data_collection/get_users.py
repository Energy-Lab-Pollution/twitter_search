import time
from pathlib import Path
from datetime import datetime

from config_utils import util
from config_utils.constants import MAX_RESULTS, COUNT_THRESHOLD, SLEEP_TIME


class UserGetter:
    """
    This class is in charge of parsing all of the twitter users
    """

    MAX_RESULTS = MAX_RESULTS
    COUNT_THRESHOLD = COUNT_THRESHOLD
    SLEEP_TIME = SLEEP_TIME

    def __init__(self, location, input_file, output_file):
        self.location = location
        self.gmaps_client = util.gmaps_client()
        self.input_file = input_file
        self.output_file = output_file

    def get_users_fromlists(self, client, lists_data, k=None):
        """
        Getting users from lists

        Parameters
        ----------
        client : TweePy API object
            Client to access Twitter API
        lists_data : _type_
            _description_
        output_file : _type_
            _description_
        k : int, optional
            _description_, by default None
        """
        unique = set()
        count = 0
        if k is None:
            k = len(lists_data)
        for item in lists_data[:k]:
            try:
                list_id = item
                print("list_id", list_id)
                if list_id is not None and list_id not in unique:
                    unique.add(list_id)
                    users = client.get_list_members(
                        id=list_id, max_results=99, user_fields=util.USER_FIELDS
                    )
                    user_dicts = util.user_dictmaker(users.data)
                    for user in user_dicts:
                        user["geo_location"] = self.get_coordinates(
                            user["location"]
                        )
                    util.json_maker(self.output_file, user_dicts)

            except Exception as e:
                print(f"Error fetching users for list {item}: {e}")
                continue
            count += 1
            if count > self.COUNT_THRESHOLD:
                print("You have to wait for 15 mins")
                time_block = 1
                while time_block <= 3:
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    time.sleep(self.SLEEP_TIME)
                    print(
                        f"{current_time} - {time_block * 5} minutes done out of 15"
                    )
                    time_block += 1
                count = 0
            time.sleep(1)
            # TODO
            # client = util.client_creator()

    def get_coordinates(self, bio_location):
        if bio_location is None:
            return (None, None)
        try:
            # Geocode the location using Google Maps Geocoding API
            geocode_result = self.gmaps_client.geocode(bio_location)

            # Check if any results were returned
            if geocode_result:
                lat = geocode_result[0]["geometry"]["location"]["lat"]
                lng = geocode_result[0]["geometry"]["location"]["lng"]
                return (lat, lng)
            else:
                return (None, None)
        except Exception as e:
            print(f"Error geocoding location '{bio_location}': {e}")
            return (None, None)

    def get_users(self):
        try:
            print("till here")
            lists_data = util.load_json(self.input_file)
            print(lists_data[:10], "here you go")
            client = util.client_creator()
            print("client created")
            isolated_lists = util.flatten_and_remove_empty(lists_data)
            print(len(isolated_lists))
            # filtered_lists = util.list_filter_keywords(isolated_lists, self.location)
            # print(len(filtered_lists))
            self.get_users_fromlists(client, isolated_lists)

        except Exception as e:
            print(f"An error occurred: {e}")
