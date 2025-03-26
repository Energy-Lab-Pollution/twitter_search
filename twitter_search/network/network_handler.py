"""
Script that handles the 'user_network.py' script to
generate a network from a particular city
"""

import os
from datetime import datetime
from pathlib import Path

from network.user_network import UserNetwork

class TwikitDataHandler:
    """
    Class that handles the Twikit search and data collection process
    """

    def __init__(self, location):
        self.location = location.lower()
        self.base_dir = Path(__file__).parent.parent / "data/networks"
