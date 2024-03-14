from twitter_search.etl.data_collection import search_users, get_lists, get_users
from twitter_search.etl.data_cleaning import clean_users

def run_search_twitter(query,location):
    x = search_users.search_users(query, location)
    y = get_lists.get_lists(x , location)
    z = get_users.get_users(y, location)
    a = clean_users.clean(z, location)
    print(a)
    #TODO
    #analyze users
    #learning method to classify users