"""
Adding script to finetune the HF NLP model
"""
from pathlib import Path

import pandas as pd
from config_utils.constants import HUGGINGFACE_MODEL, RELEVANT_LABELS
from transformers import BartTokenizer

script_path = Path(__file__).resolve()
project_root = script_path.parents[3]
CLEAN_DATA_PATH = project_root / "data" / "labeled_data"


tokenizer = BartTokenizer.from_pretrained(HUGGINGFACE_MODEL)


def labeled_data_to_csv():
    """
    Converts excel file to csv file
    """

    labeled_data = pd.read_excel(f"{CLEAN_DATA_PATH}/users_to_label.xlsx",
                                 sheet_name="Random Sample")
    labeled_data.to_csv(f"{CLEAN_DATA_PATH}/users_to_label.csv", index=False)
    return labeled_data


def preprocess_function(examples):
    """
    Preprocesses labeled data with HF and Transformers
    library
    """
    preprocessed_data = tokenizer(examples['text'], truncation=True,
                                  padding='max_length')

    return preprocessed_data


labeled_data = labeled_data_to_csv()
