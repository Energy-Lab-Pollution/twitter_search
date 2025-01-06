"""
Pipeline to search twikit users
"""
import asyncio

from twikit import Client

from config_utils import constants, util

QUERY = """location ((air pollution) OR pollution OR (public health)
                OR (poor air) OR asthma OR polluted OR (pollution control board)
                OR smog OR (air quality)) -is:retweet"""

class TwikitUserSearcher:

    def __init__(self, output_file_users, output_file_tweets, query=None):
        self.output_file_users = output_file_users
        self.output_file_tweets = output_file_tweets
        
        self.client = Client("en-US")
        self.client.load_cookies("../../twitter_search/config_utils/cookies.json")
        self.threshold = 25

    @staticmethod
    def parse_tweets_and_users(tweets):
        """
        Parses tweets and users from twikit
        """
        tweets_list = []
        users_list = []
        for tweet in tweets:
            tweet_dict = {}
            tweet_dict["tweet_id"] = tweet.id
            tweet_dict["text"] = tweet.text
            tweet_dict["created_at"] = tweet.created_at
            tweet_dict["author_id"] = tweet.user.id

            user_dict = {}
            user_dict["user_id"] = tweet.user.id
            user_dict["username"] = tweet.user.name
            user_dict["description"] = tweet.user.description
            user_dict["location"] = tweet.user.location
            user_dict["name"] = tweet.user.screen_name
            user_dict["url"] = tweet.user.url
            user_dict["tweets"] = [tweet.text]
            user_dict["geo_code"] = []

            tweets_list.append(tweet_dict)
            users_list.append(user_dict)

        return tweets_list, users_list

    async def search_tweets_and_users(self):
        """
        Method used to search for tweets with the given query
        """

        tweets = await self.client.search_tweet(self.query, "Latest", count=20)
        self.tweets_list, self.users_list = self.parse_tweets_and_users(tweets)

        more_tweets_available = True
        num_iter = 1

        next_tweets = await tweets.next()
        if next_tweets:
            next_tweets_list, next_users_list = self.parse_tweets_and_users(next_tweets)
            self.tweets_list.extend(next_tweets_list)
            self.users_list.extend(next_users_list)
        else:
            more_tweets_available = False

        while more_tweets_available:
            next_tweets = await next_tweets.next()
            if next_tweets:
                next_tweets_list, next_users_list = self.parse_tweets_and_users(next_tweets)
                self.tweets_list.extend(next_tweets_list)
                self.users_list.extend(next_users_list)

            else:
                more_tweets_available = False

            if num_iter % 5 == 0:
                print(f"Processed {num_iter} batches")

            if num_iter == self.threshold:
                break

            num_iter += 1


    def store_users_and_tweets(self):
        """
        Stores users and tweets into the provided path
        """
        util.json_maker(self.output_file_users, self.users_list)
        util.json_maker(self.output_file_tweets, self.users_list)
        

    def run_search(self):
        """
        Runs the entire search pipeline
        """
        asyncio.run(self.search_tweets_and_users())
        if not self.users_list:
            return
        self.store_users_and_tweets()
        

if __name__ == "__main__":
    query = QUERY.replace("location", "New York")
    twikit_searcher = TwikitUserSearcher("twikit-users.json", "twikit-tweets.json",
                                         query=QUERY)
    
    twikit_searcher.run_search()
