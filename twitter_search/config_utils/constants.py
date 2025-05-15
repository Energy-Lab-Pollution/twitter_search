"""
This script contains constants used across the project.
"""

from pathlib import Path

from config_utils.queries import QUERIES_EN


# Lists_filtering constants

script_path = Path(__file__).resolve()
project_root = script_path.parents[2]
analysis_project_root = script_path.parents[1]

# S3 Constants
BUCKET_NAME = "global-rct-users"
REGION_NAME = "us-west-1"

# SQS Constants
AWS_SQS_USER_TWEETS_URL = "https://sqs.us-west-1.amazonaws.com/597088024424/UserTweets"

# Construct the path to the cleaned_data directory
RAW_DATA_PATH = project_root / "data" / "raw_data"
CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"

# ACCOUNT TYPES
ACCOUNT_TYPES = list(QUERIES_EN.keys())
ACCOUNT_TYPES.append("all")

# RELEVANT USER COLUMNS
USER_COLUMNS = [
    "user_id",
    "username",
    "name",
    "description",
    "user_url",
    "location",
    "search_location",
    "tweet_count",
    "followers_count",
    "following_count",
    "listed_count",
    "tweets",
    "token",
]

# NLP CONSTANTS

HUGGINGFACE_PIPELINE = "zero-shot-classification"
HUGGINGFACE_MODEL = "facebook/bart-large-mnli"
SCORE_THRESHOLD = 0.4
NUM_WORKERS = 8
BATCH_SIZE = 8

# NLP Model Classification Labels
RELEVANT_LABELS = [
    "environment or pollution",
    "environmental research",
    "politician or policymaker",
    "nonprofit organization",
    "news outlet or journalist",
    "healthcare professional",
    "other",
]

# OTHER CONSTANTS
LIST_FIELDS = ["id", "name", "description"]
USER_FIELDS = [
    "created_at",
    "description",
    "id",
    "location",
    "most_recent_tweet_id",
    "name",
    "pinned_tweet_id",
    "profile_image_url",
    "protected",
    "public_metrics",
    "url",
    "username",
    "verified",
]

# CONSTANTS FOR SEARCH USERS AND GET LISTS SCRIPTS
MAX_RESULTS = 99
MAX_TWEETS_FROM_USERS = 5
MAX_RESULTS_LISTS = 24
EXPANSIONS = ["author_id", "entities.mentions.username", "geo.place_id"]
TWEET_FIELDS = [
    "attachments",
    "author_id",
    "context_annotations",
    "conversation_id",
    "created_at",
    "edit_controls",
    "entities",
    "geo",
    "id",
    "in_reply_to_user_id",
    "lang",
    "public_metrics",
    "possibly_sensitive",
    "referenced_tweets",
    "reply_settings",
    "source",
    "text",
    "withheld",
]

# THRESHOLDS FOR GETTING LISTS
COUNT_THRESHOLD = 240
SLEEP_TIME = 120
GEOCODE_TIMEOUT = 5

# Creating punctuations to be removed
PUNCTUATIONS = ["!" "," "." "," '"' "?" ":"]
PUNC = ""
for char in PUNCTUATIONS:
    PUNC += char

# Twikit and Network Construction Constants
# TODO: rename to account # 1, etc
TWIKIT_COOKIES_DIR = "twitter_search/config_utils/cookies.json"
TWIKIT_COOKIES_DIR_2 = "twitter_search/config_utils/fdm_cookies.json"
TWIKIT_COOKIES_DIR_3 = "twitter_search/config_utils/zm_cookies.json"
TWIKIT_COOKIES_DIR_4 = "twitter_search/config_utils/lg_cookies.json"
TWIKIT_COOKIES_DIR_5 = "twitter_search/config_utils/cookies_5.json"
TWIKIT_COOKIES_DICT = {
    "account_1": TWIKIT_COOKIES_DIR,
    "account_2": TWIKIT_COOKIES_DIR_2,
    "account_3": TWIKIT_COOKIES_DIR_3,
    "account_4": TWIKIT_COOKIES_DIR_4,
    "account_5": TWIKIT_COOKIES_DIR_5,
}
SINGLE_ACCOUNT_THRESHOLD = 10
TWIKIT_TWEETS_THRESHOLD = 50
TWIKIT_FOLLOWERS_THRESHOLD = 50
TWIKIT_RETWEETERS_THRESHOLD = 500
TWIKIT_USER_ATTRIBUTES_THRESHOLD = 400
TWIKIT_TWEETS_PER_REQUEST = 20
# Number of followers required to be processed
INFLUENCER_FOLLOWERS_THRESHOLD = 100
# Number of tweets / followers to get
TWIKIT_COUNT = 1000
FIFTEEN_MINUTES = 900
SIXTEEN_MINUTES = 960
SEVENTEEN_MINUTES = 1020
TWO_MINUTES = 120
ONE_MINUTE = 60

# X Constants
X_MAX_USER_TWEETS = 1000
X_TWEETS_PAGE_SIZE = 100
X_FOLLOWERS_PAGE_SIZE = 200
X_MAX_RETWEETERS = 500
X_MAX_FOLLOWERS = 1000

# Neptune constants
NEPTUNE_ENDPOINT = (
    "grct-test-db.cluster-cz8qgw2s68ic.us-east-2.neptune.amazonaws.com"
)
# Increases weight for existing edges in Retweet Network
RETWEET_TEMPLATE = """
g.V('{source}').fold().
  coalesce(unfold(),
           addV('user').property(id, '{source}')…property('location', '{location}')
  ).as('s').
  sideEffect(
    g.V('{target}').fold().
      coalesce(unfold(),
               addV('user').property(id, '{target}')…property('location', '{location}')
      )
  ).as('t').
  choose(
    __.select('s').outE('retweeted').where(inV().as('t')),
    __.select('s').outE('retweeted').where(inV().as('t')).
      property('weight', __.math('weight+1')).
      property(list, 'tweet_ids', '{tweet_id}'),
    __.addE('retweeted').from('s').to('t').
      property('weight', 1).
      property(list, 'tweet_ids', '{tweet_id}').
      property('location', '{location}')
  )
"""
FOLLOWER_TEMPLATE = """
g.V('{source}').fold().
  coalesce(unfold(),
           addV('user').property(id, '{source}')…property('location', '{location}')
  ).as('s').
  sideEffect(
    g.V('{target}').fold().
      coalesce(unfold(),
               addV('user').property(id, '{target}')…property('location', '{location}')
      )
  ).as('t').
  // simply add a follows edge if not exists
  coalesce(
    select('s').outE('follows').where(inV().as('t')),
    addE('follows').from('s').to('t').property('location', '{location}')
  )
"""
RETWEET_CYPHER_TEMPLATE = """
// 1. Ensure source user node exists
MERGE (s:User {id: $source})
  ON CREATE SET
    s.username  = $source_username,
    s.followers = $source_followers,
    s.location  = $location

// 2. Ensure target user node exists
MERGE (t:User {id: $target})
  ON CREATE SET
    t.username  = $target_username,
    t.followers = $target_followers,
    t.location  = $location

// 3. Ensure (or update) the retweet relationship
MERGE (s)-[r:RETWEETED { location: $location }]->(t)
  ON CREATE SET
    r.weight    = 1,
    r.tweet_ids = [$tweet_id]
  ON MATCH SET
    r.weight    = r.weight + 1,
    r.tweet_ids = r.tweet_ids + [$tweet_id]
"""

FOLLOWER_CYPHER_TEMPLATE = """// 1. Ensure source user node exists
MERGE (s:User {id: $source})
  ON CREATE SET
    s.username  = $source_username,
    s.followers = $source_followers,
    s.location  = $location

// 2. Ensure target user node exists
MERGE (t:User {id: $target})
  ON CREATE SET
    t.username  = $target_username,
    t.followers = $target_followers,
    t.location  = $location

// 3. Ensure the follows relationship exists
MERGE (s)-[f:FOLLOWS { location: $location }]->(t)
"""

NEPTUNE_S3_BUCKET = "global-rct-network-data"
IAM_ROLE_ARN = "arn:aws:iam::597088024424:role/NeptuneLoadRole"
NEPTUNE_AWS_REGION = "us-east-2"
