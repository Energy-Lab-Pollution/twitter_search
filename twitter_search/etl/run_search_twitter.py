"""Module for running Twitter search and data collection.

Author : praveenc@uchicago.edu/mahara1995@gmail.com
"""

from twitter_search.etl.data_collection import search_users, get_lists, get_users
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

    x = search_users.search_users(query, location)
    y = get_lists.get_lists(x, location)
    z = get_users.get_users(y, location)
    a = clean_users.clean(z, location)
    print(a)
    # TODO
    # analyze users
    # learning method to classify users
