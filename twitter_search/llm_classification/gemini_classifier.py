"""
Code with generative model
"""
import google.generativeai as genai

# Local imports
from keys import GEMINI_KEY
from constants import GEMINI_MODEL, GEMINI_PROMPT

# Parameters
genai.configure(api_key=GEMINI_KEY)


class GeminiClassifier:
    def __init__(self, model=GEMINI_MODEL):
        self.model = model
        # Prompts
        self.user_content = GEMINI_PROMPT

    def send_prompt(
        self,
        user_description,
        user_tweets,
    ):
        """
        Send prompt to Gemini and parse response to DataFrame
        """
        self.user_content = self.user_content.replace("user_description", user_description)
        self.user_content = self.user_content.replace("user_tweets", user_tweets)
        print("Sending prompt to Gemini...")
        model = genai.GenerativeModel(self.model)
        response = model.generate_content(self.user_content)
        self.content = response.text

    def run(self, save=True):
        """
        Run the agent
        """
        self.send_prompt()