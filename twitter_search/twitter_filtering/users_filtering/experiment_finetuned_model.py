"""
Adding script to play with the finetuned model
"""

import torch
from transformers import pipeline


# from config_utils.config import HF_TOKEN

RELEVANT_LABELS = [
    "environment or pollution",
    "environmental research",
    "politician or policymaker",
    "nonprofit organization",
    "news outlet or journalist",
    "other",
]

device = 0 if torch.cuda.is_available() else -1


# tokenizer = AutoTokenizer.from_pretrained("facebook/bart-large")

classifier = pipeline(
    "zero-shot-classification",
    model="federdm/twitter-finetuned-bart",
    hf_token="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",
    # tokenizer=tokenizer,
)

text = "Pollution"
results = classifier(text, RELEVANT_LABELS, num_workers=1)
print(results)
