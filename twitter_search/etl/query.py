"""
This script is used to create class Query. Query class' attributes help in building the query for the required use-case
"""


class Query:


    def __init__(self,location,type):
        self.location = location
        self.type = type
        self.text = self.query_builder(location,type)


    def query_builder(self,location,type):
        if type == 'media':
                return f"(media {location} OR reporter {location} OR {location} journalism OR {location} \
                        news OR {location} publications OR news agency {location}) \
                        (#news OR #media OR #journalism OR #reporter OR #journalist) -is:retweet"
        elif type == 'organizations':
            return f"(NGO {location} OR organization {location} OR non-profit {location} OR \
                    {location} OR {location} institution OR non-governmental organization) \
                    (#non-profit OR #NGO OR #NPO)\
                    -is:retweet"
        elif type == 'policymaker':
            return f"(member of parliament OR minister OR magistrate OR District magistrate OR IAS OR officer OR cabinet OR mayor OR councillor OR localgovernment OR city official OR MLA OR MP)  \
                    ({location} OR {location} government OR {location} council OR {location} municipality) \
                    (#MP OR #MLA OR #cabinet OR #minister OR #seceretary OR #IAS OR #IPS)\
                    -is:retweet"
        elif type == 'politicians':
            return f"(politics OR politicians) \
                    ({location} OR {location} politics OR {location} government) \
                    (#politics OR #politician OR #election)\
                    -is:retweet"
        elif type == 'researcher':
            return f"(environmental researcher OR health researcher OR academic OR researcher OR studying) \
                    (environment OR health OR research OR #science OR #academic) \
                    ({location} OR {location} research OR {location} science OR {location} academic) \
                    -is:retweet"
        else:
            return None