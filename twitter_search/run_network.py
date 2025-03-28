"""
Script to get followers and retweeters from a particular set
of X Users
"""
from network.network_handler import NetworkHandler

if __name__ == "__main__":
    network_handler = NetworkHandler("kolkata", 1)
    network_handler.run()
