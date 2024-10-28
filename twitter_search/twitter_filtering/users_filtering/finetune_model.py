"""
Adding script to finetune the HuggingFace NLP model
"""

from custom_models.model_finetuner import ModelFinetuner


if __name__ == "__main__":
    model_finetuner = ModelFinetuner()
    model_finetuner.train()
