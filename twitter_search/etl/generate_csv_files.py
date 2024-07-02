"""
Detects JSON files in a specified location and converts them into CSV files.
"""

from argparse import ArgumentParser

from data_cleaning.csv_converter import CSVConverter


CITIES = [
    "bangalore",
    "bangkok",
    "chennai",
    "chiang mai",
    "delhi",
    "guatemala",
    "kanpur",
    "kigali",
    "kolkata",
    "mumbai",
]


parser = ArgumentParser(
    description="Specify location to convert JSON files into CSV files."
)
parser.add_argument(
    "location",
    type=str,
    help="Specify the location to look for its JSON files.",
)

args = parser.parse_args()

if __name__ == "__main__":
    if args.location == "all":
        for city in CITIES:
            converter = CSVConverter(city)
            converter.run()
    else:
        converter = CSVConverter(args.location)
        converter.run()
