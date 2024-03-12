import time
from twitter_search.config_utils import util


def getlists_fromusers(client, users_list, k = None):

    if k is None:
        k = len(users_list) - 1
    response_user_list = []
    count = 0
    for user in users_list[:k]:
        try:
            response_user_list += client.get_list_memberships\
                (id=user.id, list_fields=util.LIST_FIELDS, max_results=50)
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
    return response_user_list

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
        client = util.client_creator()
        users_json_path = "GRCT/data/raw_data/{location}_users.json"  
        users_list = util.load_json(users_json_path)
        print("Now obtaining lists that the users are a part of")
        all_lists = getlists_fromusers(client, users_list)
        cleaned_lists = isolate_lists(all_lists)
        list_dicts = util.list_dictmaker(cleaned_lists)

        util.json_maker(f"GRCT/data/raw_data/{location}_lists.json",\
                         list_dicts)

    except Exception as e:
        print(f"An error occurred: {e}")

