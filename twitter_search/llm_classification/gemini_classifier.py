"""
Code with generative model
"""
import google.generativeai as genai

# Local imports
from keys import GEMINI_KEY
from constants import MODEL, PROMPT

# Parameters
genai.configure(api_key=GEMINI_KEY)


class GeminiAgent:
    def __init__(self, model=MODEL):
        self.model = model
        # Prompts
        self.user_content = PROMPT

    def send_prompt(
        self,
        news_article,
        given_query,
    ):
        """
        Send prompt to Gemini and parse response to DataFrame
        """
        self.user_content = self.user_content.replace("news_article_str", news_article)
        self.user_content = self.user_content.replace("given_query", given_query)
        print("Sending prompt to Gemini...")
        model = genai.GenerativeModel(self.model)
        response = model.generate_content(self.user_content)
        self.content = response.text

    def run(self, save=True):
        """
        Run the agent
        """
        self.send_prompt()