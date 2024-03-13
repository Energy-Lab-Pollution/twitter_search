import time
from twitter_search.config_utils import util
from pathlib import Path



def getlists_fromusers(client, users_list, output_file, k=None):
    if k is None:
        k = len(users_list) - 1
    count = 0
    for user in users_list[:k]:
        try:
            response_user_list = client.get_list_memberships(
                id=user['user_id'], list_fields = util.LIST_FIELDS, \
                    max_results=50)
            only_lists = isolate_lists(response_user_list)
            print({user['user_id']: only_lists})
            # Append data to the JSON file for each user
            list_entries = util.list_dictmaker({user['user_id']: only_lists})
            util.json_maker(output_file, list_entries)
        except Exception as e:
            print(f"Incomplete, currently at user {count}. Error: {e}")
        count += 1
        if count > 24:
            print("You have to wait for 15 mins")
            i = 1
            while i <= 3:
                time.sleep(300)
                print(f"{i} * 5 minutes done out of 15")
                i += 1
            count = 0
        time.sleep(1)


def isolate_lists(uncleaned_list):
    isolated_lists = []
    for sublist in uncleaned_list:
        try:
            if sublist[0].id:
                if sublist not in isolated_lists:
                    isolated_lists += sublist
        except:
            continue
    return isolated_lists

def get_lists(location):
    try:
        dir = (Path(__file__).parent.parent.parent / 
            "data/raw_data")
        output_file = dir / f"{location}_lists.json"
        input_file = dir / f"{location}_users.json"

        client = util.client_creator()
        users_list = util.load_json(input_file)
        print("Now obtaining lists that the users are a part of",users_list)
        getlists_fromusers(client, users_list, output_file)
        #cleaned_lists = isolate_lists(all_lists)
        #list_dicts = util.list_dictmaker(all_lists)

        #util.json_maker_lists(output_file,list_dicts)

    except Exception as e:
        print(f"An error occurred: {e}")
