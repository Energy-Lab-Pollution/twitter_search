from pathlib import Path
from twitter_search.config_utils import util
import time

def get_users_fromlists(client, lists_data, output_file, k=None):
    unique = set()
    count = 0
    if k is None:
        k = len(lists_data) - 1
    for item in lists_data[:k]:
        try:
            list_id = item.get('list_id')
            if list_id is not None and list_id not in unique:
                unique.add(list_id)
                users = client.get_list_members(id=list_id, max_results=50,\
                                                 user_fields=util.USER_FIELDS)
                print(users)
                user_dicts = util.user_dictmaker(users.data)
                util.json_maker(output_file, user_dicts)
        except Exception as e:
            print(f"Error fetching users for list {item.get('list_id')}: {e}")
            continue
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



def get_users(location):
    try:
        dir = (Path(__file__).parent.parent.parent / 
            "data/raw_data")
        input_file = dir / f"{location}_lists.json"
        output_file = dir / f"{location}_totalusers.json"
        #loan
        print("till here")
        lists_data = util.load_json(input_file)
        #print(lists_data,'here you go')  
        client = util.client_creator()
        print("client created")
        isolated_lists = util.flatten_and_remove_empty(lists_data)
        get_users_fromlists(client, isolated_lists, output_file)

    except Exception as e:
        print(f"An error occurred: {e}")