"""
Detects JSON files in a specified location and converts them into CSV files.
"""

from argparse import ArgumentParser

from config_utils.cities import CITIES, PILOT_CITIES
from etl.data_cleaning.csv_converter import CSVConverter


parser = ArgumentParser(
    description="Specify location to convert JSON files into CSV files."
)
parser.add_argument(
    "location",
    type=str,
    help="Specify the location to look for its JSON files.",
)

parser.add_argument(
    "twikit",
    type=str,
    help="Specify if Twikit is being used or not",
    choices=["True", "False"],
)

args = parser.parse_args()

if __name__ == "__main__":
    use_twikit = True if args.twikit == "True" else False
    if args.location == "all":
        for city in CITIES:
            converter = CSVConverter(city, use_twikit)
            converter.run()
    elif args.location == "pilot_cities":
        for city in PILOT_CITIES:
            converter = CSVConverter(city, use_twikit)
            converter.run()
    else:
        converter = CSVConverter(args.location, use_twikit)
        converter.run()
