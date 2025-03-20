"""
Script to pull tweets and retweeters from a particular user, 
"""
import twikit
from config_utils.constants import TWIKIT_COOKIES_DIR

city = "Kolkata"
user_id = "1652537276"

client = twikit.Client("en-US")
client.load_cookies(TWIKIT_COOKIES_DIR)

target_user = await client.get_user_by_id(user_id)
