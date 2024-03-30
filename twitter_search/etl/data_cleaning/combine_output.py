import json
from pathlib import Path
from twitter_search.config_utils import util
from twitter_search.config_utils import constants



def combine(type, location):

    # Define input and output directories
    dir_out = Path(__file__).parent.parent.parent / "data/cleaned_data"
    dir_in = Path(__file__).parent.parent.parent / "cleaned_data"

    # Define input and output file paths
    output_file = dir_out / f"activist{location}_cleanedusers.json"
    input_normal = dir_in / f"{location}_totalusers.json"
    input_activist = dir_in / f"activist_{location}_totalusers.json"

    users = util.load_json(input_normal)
    user_activist = util.load_json(input_activist)

    users_list = util.flatten_and_remove_empty(users)
    user_activist_list = util.flatten_and_remove_empty(user_activist)

    combined_users = users_list + user_activist_list


# Example usage:
combine(type, location)


