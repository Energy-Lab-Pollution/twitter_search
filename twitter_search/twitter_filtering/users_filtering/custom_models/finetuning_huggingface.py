"""
Adding script to finetune the HF NLP model
"""
from pathlib import Path

import pandas as pd
from transformers import BartTokenizer


script_path = Path(__file__).resolve()
project_root = script_path.parents[3]
CLEAN_DATA_PATH = project_root / "data" / "labeled_data"

HUGGINGFACE_PIPELINE = "zero-shot-classification"
HUGGINGFACE_MODEL = "facebook/bart-large-mnli"
RELEVANT_LABELS = [
    "environment or pollution",
    "environmental research",
    "politician or policymaker",
    "nonprofit organization",
    "news outlet or journalist",
    "other",
]


UNDESIRED_CHARS = ["[", "]", "\n", "  "]

tokenizer = BartTokenizer.from_pretrained(HUGGINGFACE_MODEL)


def clean_tweet(tweet):
    """
    Cleans a given tweet
    """

    for undesired_char in UNDESIRED_CHARS:
        tweet = tweet.replace(undesired_char, "")

    return tweet


def create_token(description, tweets):
    """
    Creates a token by joining descriptions
    and tweets
    """
    if tweets:
        token = f"{description} {tweets}"
    else:
        if description:
            token = str(description)
        else:
            token = None
    return token


def labeled_data_to_csv():
    """
    Converts excel file to csv file
    """

    labeled_data = pd.read_excel(
        f"{CLEAN_DATA_PATH}/users_to_label.xlsx", sheet_name="Random Sample"
    )
    labeled_data.loc[:, "tweets"] = labeled_data.loc[:, "tweets"].apply(
        lambda x: clean_tweet(x)
    )

    labeled_data.loc[:, "token"] = labeled_data.apply(
        lambda x: create_token(x.description, x.tweets), axis=1
    )

    # labeled_data.to_csv(f"{CLEAN_DATA_PATH}/users_to_label.csv", index=False)
    return labeled_data


def preprocess_function(examples):
    """
    Preprocesses labeled data with HF and Transformers
    library
    """
    tokens = list(examples.loc[:, "token"])
    preprocessed_data = tokenizer(tokens, truncation=True, padding="max_length")

    return preprocessed_data


labeled_data = labeled_data_to_csv()
preprocessed_data = preprocess_function(labeled_data)
print(preprocessed_data)
