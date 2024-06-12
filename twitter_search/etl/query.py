"""
This script is used to create class Query. Query class' attributes help in building
the query for the required use-case
"""

# Changmai from Thailand
# Rwanda - Kigali


class Query:
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

    # TODO optimize the queries further by playing on twitter website.
    def query_builder(self):
        if self.account_type == "media":
            return f"""({self.location})(media OR press OR coverage OR broadcasting
                    OR alert OR breaking OR journalism OR journalist OR news
                        OR local OR news OR patrika)
                        -is:retweet"""

        elif self.account_type == "organizations":
            return f"""(NGO {self.location} OR organization {self.location}
                       OR non-profit {self.location} OR {self.location}
                       OR {self.location} institution OR non-governmental organization)
                       (#non-profit OR #NGO OR #NPO) -is:retweet"""

        elif self.account_type == "policymaker":
            return f"""(member of parliament OR minister OR magistrate OR
                    District magistrate OR IAS OR officer OR cabinet OR mayor
                    OR councillor OR localgovernment OR city official OR MLA OR MP)
                    ({self.location} OR {self.location} government
                    OR {self.location} council OR {self.location} municipality)
                    (#MP OR #MLA OR #cabinet OR #minister
                    OR #secretary OR #IAS OR #IPS) -is:retweet"""

        elif self.account_type == "politicians":
            return f"""(politics OR politicians) ({self.location}
                    OR {self.location} politics OR {self.location} government)
                    (#politics OR #politician OR #election) -is:retweet"""

        elif self.account_type == "researcher":
            return f"""{self.location} ((public heath) OR (environmental research)
                    OR (environmental researcher) OR health OR science OR academic
                    OR research) (#science OR #research OR #academic)
                    -is:retweet"""

            # TODO: Add more keywords
        elif self.account_type == "environment":
            return f"""(air pollution {self.location} OR {self.location} air
            OR {self.location} pollution OR {self.location} public health
            OR bad air {self.location} OR {self.location} asthma OR {self.location}
            polluted OR pollution control board) (#pollution OR #environment
            OR #cleanair OR #airquality) -is:retweet"""
        else:
            return None
