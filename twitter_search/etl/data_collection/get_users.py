import pandas as pd
from twitter_search.config_utils import util


def get_users_fromlists(client, list, k = None):
    final = []
    count = 0
    if k is None:
        k = len(list) - 1
    for item in list[:k]:
        try:
            users = client.get_list_members(id = item.id, max_results= 50, user_fields=user_fields)
        except Exception as e:
            print(f"Error fetching users for list {item.id}: {e}")
            continue  
        print("count:\n",count)
        count += 1
        final += users
        if count > 24:
            time.sleep(900)
            count = 0

    return final


def get_users(location):
    try:
        dir = (Path(__file__).parent.parent.parent / 
            "data/raw_data")
        input_file = dir / f"{location}_lists.json"
        output_file = dir / f"{location}_totalusers.json"

        lists_data = util.load_json(input_file)  
        client = util.client_creator()
        get_users_fromlists(client, lists_data)
        user_dict_list = util.user_dictmaker(lists_data)
        util.json_maker(f"GRCT/data/raw_data/{location}_master_users.json", user_dict_list)
        
        excel_path = f"GRCT/data/raw_data/{location}_uncleaned.xlsx" 
        util.excel_maker(user_dict_list, excel_path)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
