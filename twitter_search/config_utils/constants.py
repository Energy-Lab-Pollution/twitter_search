"""
This script contains constants used across the project.
"""

# CONSTANTS FOR THE UTIL SCRIPT

from pathlib import Path


# Lists_filtering constants

script_path = Path(__file__).resolve()
project_root = script_path.parents[2]

# S3 Constants
BUCKET_NAME = "global-rct-users"
REGION_NAME = "us-west-1"

# Construct the path to the cleaned_data directory
RAW_DATA_PATH = project_root / "data" / "raw_data"
CLEAN_DATA_PATH = project_root / "data" / "cleaned_data"

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


# OTHER CONSTANTS

LIST_FIELDS = ["id", "name", "description"]
USER_FIELDS = [
    "created_at",
    "description",
    "entities",
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
]

# TODO change the labels to more relevant stuff.

# NLP Model Classification Labels
RELEVANT_LABELS = [
    "environment or pollution",
    "environmental research",
    "politician or policymaker",
    "nonprofit organization",
    "news outlet or journalist",
    "other",
]
# CONSTANTS FOR SEARCH USERS AND GET LISTS SCRIPTS
# Dictionary mapping Indian state capitals to their respective states
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
USER_FIELDS = [
    "created_at",
    "description",
    "entities",
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
]


# THRESHOLDS FOR GETTING LISTS
COUNT_THRESHOLD = 240
SLEEP_TIME = 120
GEOCODE_TIMEOUT = 5


# KEYWORD CONSTANTS

# Creating punctuations to be removed
PUNCTUATIONS = ["!" "," "." "," '"' "?" ":"]
PUNC = ""
for char in PUNCTUATIONS:
    PUNC += char

# Twikit Constants
TWIKIT_COOKIES_DIR = "../../twitter_search/config_utils/cookies.json"
TWIKIT_THRESHOLD = 50
