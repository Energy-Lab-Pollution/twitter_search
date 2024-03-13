import json
import pandas as pd
import os
import tweepy
from twitter_search.config_utils import config

def load_json(file_path):
    with open(file_path, "r") as json_file:
        data = json.load(json_file)
    return data

LIST_FIELDS = ["id", "name", "description"]


def user_dictmaker(user_list):
    dict_list = [] 
    for user in user_list:
        values = {'user_id': user['id'], 'username': user['username'],
                  'description': user['description'], 'location': user['location'],
                  'name': user['name'], 'url': user['url']}
        values.update(user['public_metrics'])
        dict_list.append(values)
    return dict_list

def list_dictmaker(incoming_datastruct):
    dict_list = []
    for userid, lsts in incoming_datastruct.items():
        for lst in lsts:
            values = {
                'user_id': userid,
                'list_id': lst['id'],
                'name': lst['name'],
                'created_at': lst['created_at'],
                'description': lst['description'],
                'follower_count': lst['follower_count'],
                'member_count': lst['member_count'],
                'private': lst['private'],
                'owner_id': lst['owner_id']
            }
            dict_list.append(values)
    return dict_list

def client_creator():

    consumer_key = config.consumer_key
    consumer_secret = config.consumer_secret
    access_token = config.access_token
    access_token_secret = config.access_token_secret
    bearer_token = config.bearer_token
    return tweepy.Client(
        bearer_token=bearer_token,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret
    )


def json_maker(file_path, data_to_append):
    with open(file_path, "a") as f:
        json.dump(data_to_append, f, indent=1)
        f.write('\n')  


def excel_maker(dict_list, file_path):
    df = pd.DataFrame(dict_list)
    new_df = df.drop_duplicates()
    new_df.to_excel(file_path, index=False)
