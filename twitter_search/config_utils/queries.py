# QUERY DICTIONARY
QUERIES = {
    "media": """(location)(media OR press OR coverage OR broadcasting
                OR alert OR breaking news OR journalism OR journalist
                OR local OR news OR patrika) -is:retweet""",
    "organizations": """((NGO location) OR (organization location)
                       OR (non-profit location) OR (location institution) OR
                       (non-governmental organization location) OR (nonprofit location))
                       (#non-profit OR #NGO OR #NPO OR #nonprofit) -is:retweet""",
    "policymaker": """(member of parliament OR governor OR minister OR magistrate OR
                    District magistrate OR IAS OR officer OR cabinet OR mayor
                    OR councillor OR local government OR city official OR MLA OR MP)
                    (location OR location government  OR location council OR location
                    municipality) (#MP OR #MLA OR #cabinet OR #minister
                    OR #secretary OR #IAS OR #IPS) -is:retweet""",
    "politicians": """(politics OR politicians OR politician) (location
                    OR location politics OR location government)
                    (#politics OR #politician OR #election) -is:retweet""",
    "researcher": """location ((public health) OR (environmental research)
                    OR (environmental researcher) OR health OR science OR academic
                    OR research OR university) (#science OR #research OR #academic))
                    -is:retweet""",
    "environment": """((air pollution location) OR (location air)
                    OR (location pollution) OR (location public health)
                    OR (bad air location) OR (location asthma) OR (location
                    polluted) OR (pollution control board) OR (location smog)
                    OR (air quality location))
                    (#pollution OR #environment OR #cleanair OR #airquality
                    # OR #smog) -is:retweet""",
}
