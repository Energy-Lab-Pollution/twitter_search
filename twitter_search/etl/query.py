"""
This script is used to create class Query. Query class' attributes help in building
the query for the required use-case
"""

from config_utils.queries import QUERIES


# Changmai from Thailand
# Rwanda - Kigali


class Query:
    QUERIES = QUERIES

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
        if self.account_type in QUERIES.keys():
            query = QUERIES[self.account_type]
            query = query.replace("location", self.location)
            return query
        else:
            return None
