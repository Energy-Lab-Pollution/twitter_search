import time
from pathlib import Path

from twitter_search.config_utils import constants, util


def getlists_fromusers(client, users_list, output_file, k=None):
    """
    This function takes a list of users and returns the lists that they are a part of.

    Parameters
    ----------
    client : _type_
        _description_
    users_list : _type_
        _description_
    output_file : _type_
        _description_
    k : _type_, optional
        _description_, by default None

    """
    if k is None:
        k = len(users_list) - 1
    count = 0
    for user in users_list[:k]:
        try:
            response_user_list = client.get_list_memberships(
                id=user["user_id"], list_fields=util.LIST_FIELDS, max_results=50
            )
            only_lists = isolate_lists(response_user_list)
            # Append data to the JSON file for each user
            list_entries = util.list_dictmaker({user["user_id"]: only_lists})
            util.json_maker(output_file, list_entries)
        except Exception as e:
            print(f"Incomplete, currently at user {count}. Error: {e}")
        count += 1
        if count > constants.COUNT_THRESHOLD:
            print("You have to wait for 15 mins")
            i = 1
            while i <= 3:
                time.sleep(constants.SLEEP_TIME)
                print(f"{i} * 5 minutes done out of 15")
                i += 1
            count = 0
        time.sleep(1)


def isolate_lists(uncleaned_list):
    """
    TODO: Add docstring here
    """
    isolated_lists = []
    for sublist in uncleaned_list:
        try:
            if sublist[0].id:
                if sublist not in isolated_lists:
                    isolated_lists += sublist
        except Exception as e:
            print(f"Error in isolate_lists: {e}")
            continue
    return isolated_lists


def get_lists(x, location):
    """
    Reads lists of users from a JSON file, parses them
    and returns them.
    """
    try:
        dir = Path(__file__).parent.parent.parent / "data/raw_data"
        output_file = dir / f"{location}_lists.json"
        input_file = dir / f"{location}_users.json"

        client = util.client_creator()
        users_list = util.load_json(input_file)
        isolated_lists = util.flatten_and_remove_empty(users_list)
        print("Now obtaining lists that the users are a part of", isolated_lists)
        getlists_fromusers(client, isolated_lists, output_file)
        # cleaned_lists = isolate_lists(all_lists)
        # list_dicts = util.list_dictmaker(all_lists)

        # util.json_maker_lists(output_file,list_dicts)
        return x
    except Exception as e:
        print(f"An error occurred: {e}")
