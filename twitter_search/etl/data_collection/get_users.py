import time
from pathlib import Path

from twitter_search.config_utils import constants, util


def get_users_fromlists(client, lists_data, output_file, k=None):
    """
    This function takes a list of lists and returns the users that
    are a part of them.

    Parameters
    ----------
    client : _type_
        _description_
    lists_data : _type_
        _description_
    output_file : _type_
        _description_
    k : _type_, optional
        _description_, by default None
    """
    unique = set()
    count = 0
    if k is None:
        k = len(lists_data) - 1
    for item in lists_data[:k]:
        try:
            list_id = item
            print("list_id", list_id)
            if list_id is not None and list_id not in unique:
                unique.add(list_id)
                users = client.get_list_members(
                    id=list_id, max_results=50, user_fields=util.USER_FIELDS
                )
                user_dicts = util.user_dictmaker(users.data)
                util.json_maker(output_file, user_dicts)
        except Exception as e:
            print(f"Error fetching users for list {item}: {e}")
            continue
        count += 1
        if count > constants.COUNT_THRESHOLD:
            print("You have to wait for 15 mins")
            time_block = 1
            while time_block <= 3:
                time.sleep(constants.SLEEP_TIME)
                print(f"{time_block} * 5 minutes done out of 15")
                time_block += 1
            count = 0
        time.sleep(1)


def get_users(x, location):
    """
    TODO: Add docstring
    """
    try:
        dir = Path(__file__).parent.parent.parent / "data/raw_data"
        input_file = dir / f"{location}_lists.json"
        output_file = dir / f"{location}_totalusers.json"
        print("till here")
        lists_data = util.load_json(input_file)
        print(lists_data[:10], "here you go")
        client = util.client_creator()
        print("client created")
        isolated_lists = util.flatten_and_remove_empty(lists_data)
        print(len(isolated_lists))
        filtered_lists = util.list_filter_keywords(isolated_lists, location)
        print(len(filtered_lists))
        get_users_fromlists(client, filtered_lists, output_file)

        return x
    except Exception as e:
        print(f"An error occurred: {e}")
