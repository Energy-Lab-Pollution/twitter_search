import json
from googletrans import Translator
import spacy
from langdetect import detect
from googletrans import Translator
from pathlib import Path
from twitter_search.config_utils import util
from twitter_search.config_utils import constants


def tokenize(users):
    """
    This function tokenizes the name, location, and description of each user
    """
    translator = Translator()
    count = 0
    for user in users:
        print("user count", count)
        count += 1
        # Translate non-English descriptions to English
        if "description" in user:
            try:
                detected_language = detect(user["description"])
                if detected_language != "en":
                    translation = translator.translate(user["description"], dest="en")
                    user["description"] = translation.text
            except Exception as e:
                print(f"Error translating description for user: {e}")
                continue

        # Tokenize name, location, and description
        user["token"] = []
        for key in ["name", "location", "description"]:
            if key in user and user[key]:
                try:
                    tokens = user[key].strip().split()
                    user["token"].extend(
                        [
                            word.lower().translate(
                                str.maketrans("", "", constants.PUNC)
                            )
                            for word in tokens
                            if word.lower() not in constants.INDEX_IGNORE
                        ]
                    )
                except Exception as e:
                    print(f"Error processing {key} for user: {e}")

        user_token = user.get("token", "")
        max_matches = 0
        matched_category = None
        for category, keywords in constants.CATEGORIES.items():
            num_matches = sum(keyword.lower() in user_token for keyword in keywords)
            if num_matches > max_matches:
                max_matches = num_matches
                matched_category = category

        if max_matches > 0:
            user["category"] = matched_category
        else:
            user["category"] = "Uncategorized"

    return users


def clean(x, location):
    """
    This function loads the parks.json file and writes a new file
    named normalized_parks.json that contains a normalized version
    of the parks data.
    """

    dir_out = Path(__file__).parent.parent.parent / "data/cleaned_data"
    dir_in = Path(__file__).parent.parent.parent / "data/raw_data"
    output_file = dir_out / f"{location}_cleanedusers.json"
    input_file = dir_in / f"{location}_totalusers.json"

    input = util.load_json(input_file)
    user_list = util.flatten_and_remove_empty(input)
    cleaned = tokenize(user_list)

    # Write the cleaned data to a new file
    util.json_maker(output_file, cleaned)
