"""
Adding script to finetune the HF NLP model
"""
from pathlib import Path

import pandas as pd
from transformers import BartTokenizer, BartForSequenceClassification, Trainer, TrainingArguments



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

# Load tokenizer and pre-trained model
tokenizer = BartTokenizer.from_pretrained(HUGGINGFACE_MODEL)
model = BartForSequenceClassification.from_pretrained(HUGGINGFACE_MODEL,
                                                      num_labels=len(RELEVANT_LABELS))


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
            token = ""
    return token


def parsing_labeled_data():
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

    return labeled_data


def preprocess_function(token):
    """
    Preprocesses labeled data with HF and Transformers
    library
    """
    preprocessed_data = tokenizer(token, truncation=True,
                                  padding="max_length", max_length=512)

    return preprocessed_data


labeled_data = parsing_labeled_data()
# Apply the tokenizer to each 'token' entry and store the tokenized data
labeled_data["tokenized"] = labeled_data["token"].apply(lambda x:
                                                        preprocess_function(x))

# Expand 'tokenized' column into separate columns for 'input_ids' and 'attention_mask'
tokenized_columns = labeled_data["tokenized"].apply(pd.Series)
labeled_data = pd.concat([labeled_data, tokenized_columns], axis=1)

# Drop the original 'tokenized' column if no longer needed
labeled_data.drop(columns=["tokenized"], inplace=True)
print(labeled_data[["tokenized",
                    "input_ids", "attention_mask"]].head())
