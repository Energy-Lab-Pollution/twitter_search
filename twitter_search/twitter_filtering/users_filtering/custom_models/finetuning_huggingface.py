"""
Adding script to finetune the HF NLP model
"""
from pathlib import Path

import pandas as pd
from transformers import (BartForSequenceClassification, BartTokenizer,
                          Trainer, TrainingArguments)


class ModelFinetuner:
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

    script_path = Path(__file__).resolve()
    project_root = script_path.parents[3]
    CLEAN_DATA_PATH = project_root / "data" / "labeled_data"

    def __init__(self):
        # Load tokenizer and pre-trained model
        self.tokenizer = BartTokenizer.from_pretrained(self.HUGGINGFACE_MODEL)
        self.model = BartForSequenceClassification.from_pretrained(
            self.HUGGINGFACE_MODEL, num_labels=len(self.RELEVANT_LABELS)
        )

        # Define training arguments
        training_args = TrainingArguments(
            output_dir='./results',
            evaluation_strategy='epoch',
            save_strategy='epoch',
            logging_dir='./logs',
            per_device_train_batch_size=8,
            num_train_epochs=3,  # Adjust according to your needs
            learning_rate=2e-5,
            weight_decay=0.01,
        )

        # Create the Trainer
        self.trainer = Trainer(
            model=self.model,
            args=training_args,
            train_dataset=self.tokenized_dataset['train'],
        )

    def clean_tweet(self, tweet):
        """
        Cleans a given tweet
        """

        for undesired_char in self.UNDESIRED_CHARS:
            tweet = tweet.replace(undesired_char, "")

        return tweet

    @staticmethod
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

    def parsing_labeled_data(self):
        """
        Parses the excel file for posterior HF preprocessing
        """

        labeled_data = pd.read_excel(
            f"{self.CLEAN_DATA_PATH}/users_to_label.xlsx", sheet_name="Random Sample"
        )
        labeled_data.loc[:, "tweets"] = labeled_data.loc[:, "tweets"].apply(
            lambda x: self.clean_tweet(x)
        )

        labeled_data.loc[:, "token"] = labeled_data.apply(
            lambda x: self.create_token(x.description, x.tweets), axis=1
        )

        return labeled_data

    def preprocess_function(self, token):
        """
        Preprocesses labeled data with HF and Transformers
        library
        """
        preprocessed_data = self.tokenizer(
            token, truncation=True, padding="max_length", max_length=512
        )

        return preprocessed_data

    def preprocess_labeled_data(self):
        """
        Applies the entire pre-processing steps
        to the labeled data for finetuning
        """
        labeled_data = self.parsing_labeled_data()
        # Apply the tokenizer to each 'token' entry and store the tokenized data
        labeled_data["tokenized"] = labeled_data["token"].apply(
            lambda x: self.preprocess_function(x)
        )

        # Expand 'tokenized' column into separate columns for
        # 'input_ids' and 'attention_mask'
        tokenized_columns = labeled_data["tokenized"].apply(pd.Series)
        labeled_data = pd.concat([labeled_data, tokenized_columns], axis=1)

        # Drop the original 'tokenized' column
        labeled_data.drop(columns=["tokenized"], inplace=True)
        print(labeled_data[["tokenized", "input_ids", "attention_mask"]].head())
