"""
Adding file to insert a single record to AWS Neptune
"""

import threading
import time

from config_utils.constants import GREMLIN_ADD_INTERACTION, NEPTUNE_ENDPOINT
from gremlin_python.driver import client, serializer


class NeptuneWriter:
    """
    Class that creates a long running Gremlin connection
    to insert user data to Neptune
    """

    KEEP_ALIVE_INTERVAL = 600
    NEPTUNE_ENDPOINT = NEPTUNE_ENDPOINT
    GREMLIN_ADD_INTERACTION = GREMLIN_ADD_INTERACTION

    def __init__(self):
        # Define Gremlin Client
        self._connect()
        self._start_keep_alive()

    def _connect(self):
        """
        Creates a Gremlin Client connection to
        Neptune
        """
        if hasattr(self, "client"):
            try:
                self.client.close()
            except Exception:
                pass
        self.gremlin_client = client.Client(
            self.NEPTUNE_ENDPOINT,
            "g",
            message_serializer=serializer.GraphSONSerializersV2d0(),
        )

    def _start_keep_alive(self):
        """
        Keeps Gremlin client alive

        Code suggested by ChatGPT
        """

        def ping_loop():
            while True:
                try:
                    self.gremlin_client.submitAsync("g.V().limit(1)").result()
                except Exception as e:
                    # handle reconnect if needed
                    print("Keep-alive ping failed:", e)
                time.sleep(self.KEEP_ALIVE_INTERVAL)

        t = threading.Thread(target=ping_loop, daemon=True)
        t.start()

    def add_interaction(
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
        Adds or updates vertices and creates an edge representing an interaction.

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
        query = GREMLIN_ADD_INTERACTION.format(
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
            result = self.client.submitAsync(query).result().all().result()
            return result
        except Exception as e:
            # On error: attempt reconnect once and retry
            print(
                f"[AddInteraction] Error executing query: {e}. Reconnecting and retrying..."
            )
            self._connect()
            retry_result = (
                self.client.submitAsync(query).result().all().result()
            )
            return retry_result

    def close(self):
        """Closes the Gremlin client connection."""
        try:
            self.client.close()
        except Exception as e:
            print(f"[Close] Error closing client: {e}")
