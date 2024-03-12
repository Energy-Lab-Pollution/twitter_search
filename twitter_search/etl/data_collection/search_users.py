from twitter_search.config_utils import util

def search_tweets(client, query, MAX_RESULTS, EXPANSIONS, TWEET_FIELDS, USER_FIELDS):
    return client.search_recent_tweets(query=query, max_results=MAX_RESULTS,
                                       expansions=EXPANSIONS,
                                       tweet_fields=TWEET_FIELDS,
                                       user_fields=USER_FIELDS)

def get_users_from_tweets(tweets):
    return tweets.includes['users']

def search_users(query,location):
    try:
        
        client = util.client_creator()
        print("Client initiated")
        print("Now searching for tweets")
        MAX_RESULTS = 100  
        EXPANSIONS = ["author_id", "entities.mentions.username", "geo.place_id"]
        TWEET_FIELDS = ['attachments', 'author_id', 'context_annotations', 'conversation_id', 'created_at', 'edit_controls', 'entities', 'geo', 'id', 'in_reply_to_user_id', 'lang', 'public_metrics', 'possibly_sensitive', 'referenced_tweets', 'reply_settings', 'source', 'text', 'withheld']
        USER_FIELDS = ["created_at", 'description', 'entities', 'id', 'location', 'most_recent_tweet_id', 'name', 'pinned_tweet_id', 'profile_image_url', 'protected', 'public_metrics', 'url', 'username']

        search_tweets_result = search_tweets(client, query, MAX_RESULTS, EXPANSIONS, TWEET_FIELDS, USER_FIELDS)
        total_users = get_users_from_tweets(search_tweets_result)
        total_users_dict = util.user_dictmaker(total_users)
        util.json_maker(f"GRCT/data/raw_data/{location}_users.json", total_users_dict)
        print("Total number of users:", len(total_users))

    except Exception as e:
        print(f"An error occurred: {e}")


