PROMPT="""
You are analyst with the task of determining if a given twitter
user belongs to the provided categories. We will provide the user's
description, along with several of the user's original tweets.
With this information, please determine to which category the user belongs.

You should only use the 'other' category if you feel the user
does not fit into any of the other classifications.

The categories are the following:

- "media": for journalists, media outlets, etc.
- "organizations": for NGOs, non-profits, institutions, etc.
- "policymaker": Like regulators, state treasurers,
- "politicians": for presidents, mayors, members of cabinet, etc.
- "researchers": for PhD students, postdocs, researchers (as the name suggests)
- "healthcare": for doctors, physicians, psychiatrists, surgeons, medical practitioners, etc.S

The user's description is the following:

user_description

The user's tweets are the following:

user_tweets

Use this JSON schema to return the result:

Score = {'score': float}
Return: score
"""

ROLE=""
OPENAI_MODEL="gpt-4o-mini"
GEMINI_MODEL="gemini-2.0-flash"