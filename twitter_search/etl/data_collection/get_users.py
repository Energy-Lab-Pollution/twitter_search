import pandas as pd
from twitter_search.config_utils import util

def get_users(location):
    try:
        lists_json_path = "GRCT/data/raw_data/{location}_lists.json" 
        lists_data = util.load_json(lists_json_path)  

        user_dict_list = util.user_dictmaker(lists_data)
        util.json_maker(f"GRCT/data/raw_data/{location}_master_users.json", user_dict_list)
        
        excel_path = f"GRCT/data/raw_data/{location}_uncleaned.xlsx" 
        util.excel_maker(user_dict_list, excel_path)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
