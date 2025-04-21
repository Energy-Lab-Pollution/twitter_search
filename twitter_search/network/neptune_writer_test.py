"""
Adding file to insert a single record to AWS Neptune
"""
import websocket
from gremlin_python.driver import client, serializer
from amazon_sigv4_auth import SigV4RequestAuth  # e.g. from requests-aws4auth or aws-requests-auth

from keys import aws_keys

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

def signed_transport_factory():
    headers = auth.get_signed_headers()
    header_list = [f"{k}: {v}" for k, v in headers.items()]

    class SignedWebSocketClient(WebSocketClient):
        def __init__(self, *args, **kwargs):
            kwargs['headers'] = header_list
            super().__init__(*args, **kwargs)

    return lambda *args, **kwargs: SignedWebSocketClient(*args, **kwargs)



class NeptuneWriterTest:
    """
    Class that creates a  Gremlin connection
    to insert user data to Neptune
    """

    KEEP_ALIVE_INTERVAL = 600
    NEPTUNE_ENDPOINT = NEPTUNE_ENDPOINT

    def __init__(self):
        # Define Gremlin Client
        self._connect()

    def _connect(self):
        """
        Creates a Gremlin Client connection to
        Neptune
        """
        if hasattr(self, "client"):
            try:
                self.gremlin_client.close()
            except Exception:
                pass

        auth = SigV4RequestAuth(
        aws_keys['access_key'], 
        aws_keys['secret_key'], 
        aws_region='us-east-2',
        service='neptune-db',
    )

        self.gremlin_client = client.Client(
            NEPTUNE_ENDPOINT,
            'g',
            message_serializer=serializer.GraphSONSerializersV2d0(),
            transport_factory=lambda: WebSocketClientFactory(
                url=NEPTUNE_ENDPOINT,
                headers=auth.get_signed_headers(),
            )
        )

    def _execute(self, query):
        """
        Executes a Gremlin Query
        """
        return self.gremlin_client.submitAsync(query).result().all().result()

    def add_retweet(
        self,
        source,
        source_username,
        source_followers,
        target,
        target_username,
        target_followers,
        tweet_id,
        location,
    ):
        """
        Adds or updates vertices and creates an edge representing a
        retweet interaction.

        Args:
            source (str): ID of the source user.
            source_username (str): Username of the source user.
            source_followers (int): Follower count of the source user.
            target (str): ID of the target user.
            target_username (str): Username of the target user.
            target_followers (int): Follower count of the target user.
            tweet_id (str): ID of the tweet for the interaction.
            location (str): Location tag for partitioning.
        """
        query = RETWEET_TEMPLATE.format(
            source=source,
            source_username=source_username,
            source_followers=source_followers,
            target=target,
            target_username=target_username,
            target_followers=target_followers,
            tweet_id=tweet_id,
            location=location,
        )

        try:
            return self._execute(query)
        except Exception as e:
            # On error: attempt reconnect once and retry
            print(
                f"[AddInteraction] Error executing query: {e}. Reconnecting and retrying..."
            )


    def add_follower(
        self,
        source,
        source_username,
        source_followers,
        target,
        target_username,
        target_followers,
        location,
    ):
        """
        Adds or updates vertices and creates an edge representing a
        follow interaction.

        Args:
            source (str): ID of the source user.
            source_username (str): Username of the source user.
            source_followers (int): Follower count of the source user.
            target (str): ID of the target user.
            target_username (str): Username of the target user.
            target_followers (int): Follower count of the target user.
            location (str): Location tag for partitioning.
        """
        query = FOLLOWER_TEMPLATE.format(
            source=source,
            source_username=source_username,
            source_followers=source_followers,
            target=target,
            target_username=target_username,
            target_followers=target_followers,
            location=location,
        )
        try:
            return self._execute(query)
        except Exception as e:
            # On error: attempt reconnect once and retry
            print(
                f"[AddInteraction] Error executing query: {e}. Reconnecting and retrying..."
            )


    def close(self):
        """Closes the Gremlin client connection."""
        try:
            self.client.close()
        except Exception as e:
            print(f"[Close] Error closing client: {e}")


if __name__ == "__main__":

    writer = NeptuneWriterTest()
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