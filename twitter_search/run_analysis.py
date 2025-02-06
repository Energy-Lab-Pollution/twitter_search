"""
Script to run the EDA Analysis we have created so far
"""

from etl.data_analysis.location_analysis import LocationAnalyzer
from etl.data_analysis.user_analysis import UserAnalyzer


if __name__ == "__main__":
    user_analyzer = UserAnalyzer()
    location_analyzer = LocationAnalyzer()

    user_analyzer.run()
    location_analyzer.run()
