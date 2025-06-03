PROMPT="""
You are analyst with the task of determining if a given 
user belongs to the provided categories. You will provide a
the category that you feel is most likely for th.

The user's description is the following:

user_description

The given query is:

given_query

Use this JSON schema to return the result:

Score = {'score': float}
Return: score
"""

ROLE=""
OPENAI_MODEL="gpt-4o-mini"
GEMINI_MODEL="gemini-2.0-flash"