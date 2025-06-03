"""
Code with GPT Agent class
"""

import re
from datetime import datetime
from openai import OpenAI

# Local
from keys import OPENAI_KEY
from constants import OPENAI_MODEL, OPENAI_INSTRUCTIONS, OPENAI_PROMPT

# Parameters
client = OpenAI(api_key=OPENAI_KEY)

class GPTAClassifier:
    def __init__(
        self, model=OPENAI_MODEL
    ):
        self.model = model
        # Read files and get ticker mapping
        # Prompts
        self.system_content = OPENAI_INSTRUCTIONS
        self.user_content = OPENAI_PROMPT

    # Define the prompt
    def send_prompt(self, user_description, user_tweets):
        """
        Sends prompt to OpenAI API and returns the response
        """
        self.user_content = self.user_content.replace("user_description", user_description)
        self.user_content = self.user_content.replace("user_tweets", user_tweets)

        messages = [
            {"role": "system", "content": self.system_content},
            {"role": "user", "content": self.user_content},
        ]

        try:
            print(f"Sending prompt to model {self.model}...")
            response = client.chat.completions.create(
                model=self.model, messages=messages
            )
            print("Prompt sent!")
        except Exception as e:
            print(f"API error - avoiding retrying: {e}")
            return

        self.content = response.choices[0].message.content

    def run(self):
        """
        Run the agent
        """
        self.send_prompt()