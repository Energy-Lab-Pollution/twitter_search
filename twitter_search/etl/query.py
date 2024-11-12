"""
This script is used to create class Query. Query class' attributes help in building
the query for the required use-case
"""

from config_utils.cities import ALIAS_DICT, CITIES_LANGS
from config_utils.queries import QUERIES_DICT


class Query:
    QUERIES_DICT = QUERIES_DICT
    CITIES_LANGS = CITIES_LANGS

    def __init__(self, location, account_type):
        self.location = location
        self.account_type = account_type
        self.text = self.query_builder()
        self.text = self.text.replace("\n", " ").strip()
        self.text = self.text.replace("  ", " ")
        self.text = self.text.replace("\t", " ")
        if self.text is not None:
            print(
                f"query built for {self.location} location and \
                  {self.account_type} account type"
            )

    def query_builder(self):
        """
        Returns the corresponding query

        The function extracts the query from the QUERIES dictionary,
        and also replaces the location with the appropiate one
        """

        print("Checking if given city is in alias dictionary")
        if self.location in ALIAS_DICT:
            print(f"{self.location} found in alias dict")
            main_city = ALIAS_DICT[self.location]

            print(f"Getting language and queries for  {self.location}- {main_city}")
            language = self.CITIES_LANGS[main_city]
            queries = QUERIES_DICT[language]

        else:
            queries = QUERIES_DICT['en']

        if self.account_type in queries.keys():
            query = queries[self.account_type]
            query = query.replace("location", self.location)
            return query
        else:
            return None
