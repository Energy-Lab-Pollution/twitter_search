"""
Script to get followers and retweeters from a particular set
of X Users
"""
import asyncio
from network.network_handler import NetworkHandler

if __name__ == "__main__":
    network_handler = NetworkHandler("kolkata", 1)
    asyncio.run(network_handler.run())
