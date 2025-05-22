"""
Script to get followers and retweeters from a particular set
of X Users
"""

from argparse import ArgumentParser

from network.network_handler import NetworkHandler


if __name__ == "__main__":
    parser = ArgumentParser(
        "Parameters to get users data to generate a network"
    )
    parser.add_argument(
        "--location", type=str, help="Location to read users from"
    )

    args = parser.parse_args()
    network_handler = NetworkHandler(args.location)
    print(f"Calculating retweet edges for {args.location}...")
    network_handler.create_edges("retweet")
    print(f"Calculating follower edges for {args.location}...")
    network_handler.create_edges("follower")
    network_handler.calculate_stats()
