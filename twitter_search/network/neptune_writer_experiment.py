"""
Adding file to insert a single record to AWS Neptune
"""

from botocore.credentials import Credentials
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from gremlin_python.driver.client import Client
from gremlin_python.driver.serializer import GraphSONSerializersV2d0
from keys import aws_keys
from websocket import create_connection


# Neptune constants
NEPTUNE_ENDPOINT = "wss://grct-test-db.cluster-cz8qgw2s68ic.us-east-2.neptune.amazonaws.com:8182/gremlin"
# Increases weight for existing edges in Retweet Network
RETWEET_TEMPLATE = """
g.V('{source}').fold().
  coalesce(unfold(),
           addV('user').property(id, '{source}')…property('location', '{location}')
  ).as('s').
  sideEffect(
    g.V('{target}').fold().
      coalesce(unfold(),
               addV('user').property(id, '{target}')…property('location', '{location}')
      )
  ).as('t').
  choose(
    __.select('s').outE('retweeted').where(inV().as('t')),
    __.select('s').outE('retweeted').where(inV().as('t')).
      property('weight', __.math('weight+1')).
      property(list, 'tweet_ids', '{tweet_id}'),
    __.addE('retweeted').from('s').to('t').
      property('weight', 1).
      property(list, 'tweet_ids', '{tweet_id}').
      property('location', '{location}')
  )
"""
FOLLOWER_TEMPLATE = """
g.V('{source}').fold().
  coalesce(unfold(),
           addV('user').property(id, '{source}')…property('location', '{location}')
  ).as('s').
  sideEffect(
    g.V('{target}').fold().
      coalesce(unfold(),
               addV('user').property(id, '{target}')…property('location', '{location}')
      )
  ).as('t').
  // simply add a follows edge if not exists
  coalesce(
    select('s').outE('follows').where(inV().as('t')),
    addE('follows').from('s').to('t').property('location', '{location}')
  )
"""


class NeptuneClient:
    KEEP_ALIVE = 600  # ping interval in seconds

    def __init__(self):
        self.endpoint = NEPTUNE_ENDPOINT
        # set up AWS4Auth correctly
        self.creds = Credentials(
            aws_keys["aws_access_key"],
            aws_keys["aws_secret_key"],
        )
        self.service="neptune-db",
        self.region="us-east-2",
        self.url = f"wss://{self.endpoint}:8182/gremlin"
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
                self.url,
                header=header_list,
                ping_interval=self.KEEP_ALIVE
            )

        self.client = Client(
            self.url,
            'g',
            message_serializer=GraphSONSerializersV2d0(),
            transport_factory=transport_factory
        )

    def _execute(self, gremlin_query: str, retry: bool = True):
        """Submits a Gremlin query, retries once on failure."""
        try:
            result = self.client.submitAsync(gremlin_query)
            return result.result().all().result()
        except Exception as e:
            self.log.warning(f"Query failed: {e}")
            if retry:
                self.log.info("Reconnecting and retrying...")
                self._connect()
                return self._execute(gremlin_query, retry=False)
            else:
                self.log.error("Retry also failed.")
                raise

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
            self.log.info("Connection closed.")
        except Exception as e:
            self.log.error(f"Error closing connection: {e}")


if __name__ == "__main__":

    writer = NeptuneClient()
    print("Initialized client...")
    writer.add_retweet(
        source="1392834695558144004",
        source_username="ratri_bose",
        source_followers=95,
        target="1433432747989626880",
        target_username="LiveSanghamitra",
        target_followers=921,
        tweet_id="1909221609862209855",
        location="kolkata",
    )
    writer.add_retweet(
        source="966616811411091456",
        source_username="AitcProvat",
        source_followers=1482,
        target="1433432747989626880",
        target_username="LiveSanghamitra",
        target_followers=921,
        tweet_id="1908827352471117987",
        location="kolkata",
    )

    writer.add_retweet(
        source="354283190",
        source_username="SureshKumarIyer",
        source_followers=58,
        target="1433432747989626880",
        target_username="LiveSanghamitra",
        target_followers=921,
        tweet_id="1908233554544271446",
        location="kolkata",
    )
    print("Added retweet edges")

    writer.add_follower(
        source="1263531549888335873",
        source_username="nghamitraLIVE",
        source_followers=19975,
        target="1433432747989626880",
        target_username="LiveSanghamitra",
        target_followers=921,
        location="kolkata",
    )

    print("Follower edge added")

    writer.close()

    print("Closed client")
