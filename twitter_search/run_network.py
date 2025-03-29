"""
Script to get followers and retweeters from a particular set
of X Users
"""
from argparse import ArgumentParser
import time
import asyncio
from config_utils.constants import FIFTEEN_MINUTES
from network.network_handler import NetworkHandler

if __name__ == "__main__":
    parser = ArgumentParser("Parameters to get users data to generate a network")
    parser.add_argument("location", type=str, help="Location to read users from")
    parser.add_argument("num_users", type=int, help="Number of users to process")
    parser.add_argument("wait", type=str, choices=["Yes", "No"], help="Decide whether to wait 15 mins or not")

    args = parser.parse_args()
    
    if args.wait == "Yes":
        print("Sleeping for 15 minutes...")
        time.sleep(FIFTEEN_MINUTES)

    network_handler = NetworkHandler(args.location, args.num_users)
    asyncio.run(network_handler.run())
