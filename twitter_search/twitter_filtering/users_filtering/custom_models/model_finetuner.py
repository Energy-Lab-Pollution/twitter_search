"""
Adding script to finetune the HuggingFace NLP model
"""
from pathlib import Path

import pandas as pd
from datasets import Dataset
from transformers import (
    BartForSequenceClassification,
    BartTokenizer,
    Trainer,
    TrainingArguments,
)


# from config_utils.constants import (HUGGINGFACE_MODEL,
#                                     HUGGINGFACE_PIPELINE,
#                                     RELEVANT_LABELS)


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
    # HUGGINGFACE_PIPELINE = HUGGINGFACE_PIPELINE
    # HUGGINGFACE_MODEL = HUGGINGFACE_MODEL
    # RELEVANT_LABELS = RELEVANT_LABELS
    UNDESIRED_CHARS = ["[", "]", "\n", "  "]
    NUM_EPOCHS = 3
    LEARNING_RATE = 2e-5
    WEIGHT_DECAY = 0.01

    script_path = Path(__file__).resolve()
    project_root = script_path.parents[3]
    CLEAN_DATA_PATH = project_root / "data" / "labeled_data"
    FINETUNING_PATH = project_root / "finetuning_results"

    def __init__(self):
        # Load tokenizer and pre-trained model
        self.tokenizer = BartTokenizer.from_pretrained(self.HUGGINGFACE_MODEL)
        self.model = BartForSequenceClassification.from_pretrained(
            self.HUGGINGFACE_MODEL,
            num_labels=len(self.RELEVANT_LABELS),
            ignore_mismatched_sizes=True,
        )

        # Define training arguments
        self.training_args = TrainingArguments(
            output_dir=f"{self.FINETUNING_PATH}/results",
            eval_strategy="epoch",
            save_strategy="epoch",
            logging_dir=f"{self.FINETUNING_PATH}/logs",
            per_device_train_batch_size=8,
            num_train_epochs=self.NUM_EPOCHS,  # Adjust according to your needs
            learning_rate=self.LEARNING_RATE,
            weight_decay=self.WEIGHT_DECAY,
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
            f"{self.CLEAN_DATA_PATH}/users_to_label.xlsx",
            sheet_name="Random Sample",
        )

        # Replace data
        labeled_data.loc[
            labeled_data["manual classification"] == "Researchers"
        ] = "environmental research"

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
        library using a pre-trained tokenizer
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
        labeled_data.drop(columns=["tokenized"], inplace=True)

        # Map labels to numeric values
        label_to_id = {label: i for i, label in enumerate(self.RELEVANT_LABELS)}
        labeled_data["labels"] = labeled_data["manual classification"].map(label_to_id)

        return labeled_data[["input_ids", "attention_mask", "labels"]]

    def split_data(self, data, test_size=0.2):
        """
        Transform pandas df to dataset and split
        into train and test
        """
        dataset = Dataset.from_pandas(data)
        split_dataset = dataset.train_test_split(test_size=test_size)
        return split_dataset["train"], split_dataset["test"]

    def train(self):
        """
        Preporceses the labeled data and finetunes the model
        """
        labeled_data = self.preprocess_labeled_data()
        train_dataset, test_dataset = self.split_data(labeled_data)

        # Initialize the Trainer with train and eval datasets
        trainer = Trainer(
            model=self.model,
            args=self.training_args,
            train_dataset=train_dataset,
            eval_dataset=test_dataset,
        )

        # Fine-tune the model
        trainer.train()

        # Evaluate the model on the test set
        eval_results = trainer.evaluate()
        print("Evaluation Results:", eval_results)
