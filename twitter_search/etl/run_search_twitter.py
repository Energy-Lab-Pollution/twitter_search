"""Module for running Twitter search and data collection.

Author : praveenc@uchicago.edu/mahara1995@gmail.com
"""

from etl.data_collection.get_users import UserGetter
from etl.data_collection.get_lists import ListGetter
from etl.data_collection.search_users import UserSearcher
from twitter_filtering.users_filtering.users import UserFilter
from etl.data_cleaning import clean_users


def run_search_twitter(query, location):
    """
    Run Twitter search and data collection process.

    This function executes a series of steps to search for Twitter users based on a query
    and location, retrieve their lists and user data, clean the collected data, and print the result.

    Args:
        query (str): The search query to search for Twitter users.
        location (str): The location to filter the search for Twitter users.

    Returns:
        str: A message indicating the completion of the data cleaning process.

    Raises:
        ValueError: If any of the input arguments are invalid.
    """

    print("Searching for Twitter users...")
    # user_searcher = UserSearcher(location,query)
    # user_searcher.run_search_all()

    #user_filter = UserFilter(location)
    #user_filter.run_filtering()

    print("Retrieving lists and users...")
    # Defince instance of the user getter class
    #list_getter = ListGetter(location)
    #list_getter.get_lists()

    print("Retrieving users from lists...")
    user_getter = UserGetter(location)
    path = user_getter.get_users()

    user_filter = UserFilter(location,path)
    user_filter.run_filtering()

    print("Cleaning user data...")
    #clean_users.clean(location)
    # TODO
    # analyze users
    # learning method to classify users
