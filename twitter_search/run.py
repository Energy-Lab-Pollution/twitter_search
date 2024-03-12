from twitter_search.etl import run_search_twitter


def build_query(location):

    return f"(air pollution {location} OR {location} air OR {location} \
        pollution OR {location} public health OR bad air {location} OR \
        {location} asthma OR {location} polluted OR pollution control board) \
        (#pollution OR #environment OR #cleanair OR #airquality) -is:retweet"

def lets_getit(location):
        
        # Build the query based on args.location
        query = build_query(location)
        run_search_twitter(query,location)

        # Additional logic or call other scripts if needed



