"""
Script to get followers and retweeters from a particular set
of X Users
"""

from network.neptune_client import NeptuneClient


if __name__ == "__main__":

    neptune_client = NeptuneClient()
    print("Initialized client...")
    neptune_client.add_retweet(
        source="1392834695558144004",
        source_username="ratri_bose",
        source_followers=95,
        target="1433432747989626880",
        target_username="LiveSanghamitra",
        target_followers=921,
        tweet_id="1909221609862209855",
        location="kolkata",
    )
    neptune_client.add_retweet(
        source="966616811411091456",
        source_username="AitcProvat",
        source_followers=1482,
        target="1433432747989626880",
        target_username="LiveSanghamitra",
        target_followers=921,
        tweet_id="1908827352471117987",
        location="kolkata",
    )

    neptune_client.add_retweet(
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

    neptune_client.add_follower(
        source="1263531549888335873",
        source_username="nghamitraLIVE",
        source_followers=19975,
        target="1433432747989626880",
        target_username="LiveSanghamitra",
        target_followers=921,
        location="kolkata",
    )

    print("Follower edge added")

    neptune_client.close()

    print("Closed client")

