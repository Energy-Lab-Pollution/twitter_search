import json
from googletrans import Translator
import spacy
from langdetect import detect
from googletrans import Translator
from pathlib import Path
from twitter_search.config_utils import util

#Creating punctuations to be removed
PUNCTUATIONS = ['!'',''.'',''"''?'':']
PUNC =""
for char in PUNCTUATIONS:
    PUNC += char

ACTIVIST_KEYWORDS = [
    "activist", "advocate", "campaigner", "crusader", "protester", "reformer", "champion", 
    "fighter", "supporter", "defender", "militant", "social justice", "human rights", "equality", 
    "protest", "demonstration", "advocacy", "grassroots", "change", "activism", "solidarity", "freedom", 
    "empowerment", "justice", "civil rights", "rights", "equality", "liberty", "justice", "fairness"
]
POLITICS_KEYWORDS = [
    "politics", "political", "government", "policy", "election", "democracy", "governance", "legislation", 
    "politician", "party", "candidate", "voting", "public office", "campaign", "administration", 
    "constituency", "parliament", "congress", "senate", "diplomacy", "public policy", "lawmaker", 
    "civic", "political science", "political party", "opposition", "congress", "administration", 
    "governance", "public affairs", "legislation", "policy-making", "executive", "legislative", "judicial"
]
MEDIA_KEYWORDS = [
    "media", "journalism", "news", "reporting", "press", "broadcast", "communication", "journalist", 
    "reporter", "editor", "publisher", "newsroom", "coverage", "broadcasting", "newscast", "information", 
    "current affairs", "headline", "correspondent", "media outlet", "coverage", "editorial", "press release", 
    "journalism", "media industry", "reporting", "newsroom", "headline", "current events", "broadcast"
]
ORGANIZATION_KEYWORDS = [
    "official", "company", "organization", "corporation", 'collective', 'group', 'association', 'enterprise', 
    'foundation', 'institute', 'team', 'society', 'union', 'network', 'coalition', 'syndicate', 'consortium', 
    'firm', 'club', 'guild', 'committee', 'bureau', 'agency', 'cooperative', 'office', 'sector', 'service', 
    'branch', 'division', 'subsidiary', 'affiliate', 'nonprofit', 'charity', 'NGO', 'non-governmental organization', 
    'advocacy', 'humanitarian', 'volunteer', 'philanthropy', 'community', 'society', 'coalition', 'alliance', 
    'initiative', 'campaign', 'movement', 'project', 'network', 'consortium', 'union', 'association', 'cooperative', 
    'collective', 'group', 'committee', 'team', 'council', 'partnership', 'collaborative', 'forum', 'guild', 
    'federation', 'civic', 'public service', 'social impact', 'sustainable', 'environmental', 'social responsibility', 
    'community service', 'global', 'international', 'worldwide', 'multinational', 'transnational', 'globalized', 
    'organization', 'corporation', 'business', 'enterprise', 'agency', 'firm', 'institute', 'association', 'group', 
    'foundation', 'committee', 'nonprofit', 'charity', 'NGO', 'collective', 'coalition', 'alliance', 'initiative', 
    'movement', 'network', 'union', 'team', 'council', 'partnership', 'collaborative', 'forum', 'service', 
    'community', 'sector'
]
GOVERNMENT_KEYWORDS = [
    "government", 'govt', 'gov', 'public sector', 'office of', "public administration", "public service", 
    "public policy", "civil service", "governmental", "state", "federal", "local government", "administration", 
    "governance", "public affairs", "regulation", "authority", "bureaucracy", "policy-making", "public office", 
    "official", "public servant", "civil servant", "legislation", "executive", "legislative", "judicial", 
    "government agency", "public institution", 'government', 'govt', 'gov', 'public sector', 'office of', 
    'administration', 'regulation', 'authority', 'bureaucracy', 'policy-making', 'public office', 'legislation', 
    'executive', 'legislative', 'judicial', 'government agency', 'institution', 'public service', 'public sector', 
    'civil service', 'local government', 'federal government', 'state government', 'public administration', 
    'governmental', 'administration', 'president', 'prime minister', 'parliament'
]

CATEGORIES = {
        "Activist": ACTIVIST_KEYWORDS,
        "Politics": POLITICS_KEYWORDS,
        "Media": MEDIA_KEYWORDS,
        "Organization": ORGANIZATION_KEYWORDS,
        "Government": GOVERNMENT_KEYWORDS
    }

# words to be ignored in indexing
INDEX_IGNORE = (
    "a",
    "an",
    "and",
    "&",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "he",
    "in",
    "is",
    "it",
    "its",
    "of",
    "on",
    "that",
    "the",
    "to",
    "was",
    "were",
    "will",
    "with"
)



# def is_english(text):
#     # Function to check if the text is in English
#     try:
#         return text.encode(encoding='utf-8').decode('ascii')
#     except UnicodeDecodeError:
#         return False

# def batch_translate_to_english(users, batch_size=10):
#     translator = Translator()

#     for i in range(0, len(users), batch_size):
#         batch = users[i:i+batch_size]

#         for user in batch:
#             for key, value in user.items():
#                 if key in ["name", "location", "description"] and value:
#                     try:
#                         # Detect the language of the text
#                         detected_language = detect(value)

#                         # Check if the detected language is not English
#                         if detected_language != 'en':
#                             translation = translator.translate(value, dest='en')
#                             user[key] = translation.text

#                     except Exception as e:
#                         print(f"Error translating {key}: {e}")
#                         continue

#     return users


def tokenize(users):
    translator = Translator()
    count = 0
    for user in users:
        print("user count",count)
        count += 1
        # Translate non-English descriptions to English
        if 'description' in user:
            try:
                detected_language = detect(user['description'])
                if detected_language != 'en':
                    translation = translator.translate(user['description'], dest='en')
                    user['description'] = translation.text
            except Exception as e:
                print(f"Error translating description for user: {e}")
                continue

        # Tokenize name, location, and description
        user['token'] = []
        for key in ["name", "location", "description"]:
            if key in user and user[key]:
                try:
                    tokens = user[key].strip().split()
                    user['token'].extend([
                        word.lower().translate(str.maketrans('', '', PUNC))
                        for word in tokens
                        if word.lower() not in INDEX_IGNORE
                    ])
                except Exception as e:
                    print(f"Error processing {key} for user: {e}")

        user_token = user.get("token", "")
        max_matches = 0
        matched_category = None
        for category, keywords in CATEGORIES.items():
            num_matches = sum(keyword.lower() in user_token for keyword in keywords)
            if num_matches > max_matches:
                max_matches = num_matches
                matched_category = category

        if max_matches > 0:
            user['category'] = matched_category
        else:
            user['category'] = "Uncategorized"


    return users


def clean(location):
    """
    This function loads the parks.json file and writes a new file
    named normalized_parks.json that contains a normalized version
    of the parks data.
    """

    dir_out = (Path(__file__).parent.parent.parent / 
        "data/cleaned_data")
    dir_in = (Path(__file__).parent.parent.parent / 
        "data/raw_data")
    output_file = dir_out / f"{location}_cleanedusers.json"
    input_file = dir_in / f"{location}_totalusers.json"

    input = util.load_json(input_file)
    user_list = util.flatten_and_remove_empty(input)
    cleaned = tokenize(user_list)
    util.json_maker(output_file,cleaned)
