"""
Adding file to insert a single record to AWS Neptune
"""

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from config_utils.constants import (
    FOLLOWER_TEMPLATE,
    NEPTUNE_ENDPOINT,
    RETWEET_TEMPLATE,
)
from gremlin_python.driver.client import Client
from gremlin_python.driver.serializer import GraphSONSerializersV2d0

# Local imports
from keys import aws_keys
from websocket import create_connection


class NeptuneClient:
    KEEP_ALIVE = 60  # ping interval in seconds

    def __init__(self):
        self.endpoint = f"wss://{NEPTUNE_ENDPOINT}:8182/gremlin"
        # set up AWS4Auth correctly
        self.creds = Credentials(
            aws_keys["aws_access_key"],
            aws_keys["aws_secret_key"],
        )
        self.service = "neptune-db"
        self.region = "us-east-2"
        self.url = NEPTUNE_ENDPOINT
        # open initial connection
        self._connect()

    def _get_sigv4_headers(self):
        # AWSRequest wants a “realistic” request object
        aws_req = AWSRequest(method="GET", url=self.url, headers={})
        SigV4Auth(self.creds, self.service, self.region).add_auth(aws_req)
        return dict(aws_req.headers.items())

    def _connect(self):
        sig_hdrs = self._get_sigv4_headers()
        header_list = [f"{k}: {v}" for k, v in sig_hdrs.items()]

        def transport_factory():
            return create_connection(
                self.url, header=header_list, ping_interval=self.KEEP_ALIVE
            )

        self.client = Client(
            self.url,
            "g",
            message_serializer=GraphSONSerializersV2d0(),
            transport_factory=transport_factory,
        )

    def _execute(self, gremlin_query: str, retry: bool = True):
        """Submits a Gremlin query, retries once on failure."""
        try:
            result = self.client.submitAsync(gremlin_query)
            return result.result().all().result()
        except Exception as e:
            print(f"Query failed: {e}")

    def add_interaction(self, template: str, **kwargs):
        """
        Generic: formats the given template with kwargs and executes it.
        """
        query = template.format(**kwargs)
        return self._execute(query)

    def add_retweet(
        self,
        source: str,
        source_username: str,
        source_followers: int,
        target: str,
        target_username: str,
        target_followers: int,
        tweet_id: str,
        location: str,
    ):
        return self.add_interaction(
            RETWEET_TEMPLATE,
            source=source,
            source_username=source_username,
            source_followers=source_followers,
            target=target,
            target_username=target_username,
            target_followers=target_followers,
            tweet_id=tweet_id,
            location=location,
        )

    def add_follower(
        self,
        source: str,
        source_username: str,
        source_followers: int,
        target: str,
        target_username: str,
        target_followers: int,
        location: str,
    ):
        return self.add_interaction(
            FOLLOWER_TEMPLATE,
            source=source,
            source_username=source_username,
            source_followers=source_followers,
            target=target,
            target_username=target_username,
            target_followers=target_followers,
            location=location,
        )

    def close(self):
        """Closes the underlying Gremlin client."""
        try:
            self.client.close()
            print("Connection closed.")
        except Exception as e:
            print(f"Error closing connection: {e}")
