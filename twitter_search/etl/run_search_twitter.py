"""Module for running Twitter search and data collection.

Author : praveenc@uchicago.edu/mahara1995@gmail.com
"""

from twitter_search.etl.data_collection.get_users import UserGetter
from twitter_search.etl.data_collection.get_lists import ListGetter
from twitter_search.etl.data_collection import search_users
from twitter_search.etl.data_cleaning import clean_users


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

    # TODO: What are those 'x', 'y', 'z', 'a' variables?

    x = search_users.search_users(query, location)

    # Defince instance of the user getter class
    list_getter = UserGetter(x, location)
    y = list_getter.get_lists(x, location)

    # Define instance of list getter class
    user_getter = ListGetter(y, location)
    z = user_getter.get_users(y, location)

    a = clean_users.clean(z, location)
    print(a)
    # TODO
    # analyze users
    # learning method to classify users
