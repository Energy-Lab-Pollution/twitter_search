GEMINI_PROMPT = """
You are an experienced analyst with the task of determining if a given twitter
user belongs to the provided categories. We will provide the user's
description, along with several of the user's original tweets.
With this information, please determine to which category the user belongs.

Also, please give more weight to the description while performing the
classification. The tweets are important, but the description usually
reveals what the user does or is.

You should only use the 'other' category if you feel the user
does not fit into any of the other classifications.

The categories are the following. I will give you a brief guide
of which 'professions' encompass such classifications, but feel free
to make your own decision.

- "media": for journalists, media outlets, etc.
- "organizations": for NGOs, non-profits, institutions, etc.
- "policymaker": Like regulators, state treasurers, attorney generals, diplomats, etc
- "politicians": for presidents, mayors, members of cabinet, etc.
- "environment": for environmental activists, pollution monitors, etc.
- "researchers": for PhD students, postdocs, researchers (as the name suggests)
- "healthcare": for doctors, physicians, psychiatrists, surgeons, medical practitioners, etc.
- "other": if none of the above fit the user

The user's description is the following:

user_description

The user's tweets are the following:

user_tweets

Also, please briefly explain why you think this user belongs to the
category you chose.

Use this JSON schema to return the result:

Category = {'category': str}
Return: category
"""

OPENAI_INSTRUCTIONS = """
You are an experienced analyst with the task of determining if a given twitter
user belongs to the provided categories. We will provide the user's
description, along with several of the user's original tweets.
With this information, please determine to which category the user belongs.

Also, please give more weight to the description while performing the
classification. The tweets are important, but the description usually
reveals what the user does or is.

You should only use the 'other' category if you feel the user
does not fit into any of the other classifications.

The categories are the following. I will give you a brief guide
of which 'professions' encompass such classifications, but feel free
to make your own decision.

- "media": for journalists, media outlets, etc.
- "organizations": for NGOs, non-profits, institutions, etc.
- "policymaker": Like regulators, state treasurers, attorney generals, diplomats, etc
- "politicians": for presidents, mayors, members of cabinet, etc.
- "environment": for environmental activists, pollution monitors, etc.
- "researchers": for PhD students, postdocs, researchers (as the name suggests)
- "healthcare": for doctors, physicians, psychiatrists, surgeons, medical practitioners, etc.
- "other": if none of the above fit the user
"""
OPENAI_PROMPT = """
The user's description is the following:

user_description

The user's tweets are the following:

user_tweets

Also, please briefly explain why you think this user belongs to the
category you chose.

Use this JSON schema to return the result:

Category = {'category': str}
Return: category
"""


OPENAI_MODEL = "gpt-4o-mini"
GEMINI_MODEL = "gemini-2.0-flash"

NEPTUNE_S3_BUCKET = "global-rct-network-data"
NEPTUNE_AWS_REGION = "us-east-2"
