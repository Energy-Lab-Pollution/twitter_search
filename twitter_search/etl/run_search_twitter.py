from twitter_search.etl.data_collection import search_users, get_lists, get_users

def run_search_twitter(query,location):
    print("here tooooo")
    search_users.search_users(query, location)
    get_lists.get_lists(location)
    get_users.get_users(location)
    #TODO 
    #clean users
    #classify users