"""
Detects all generated .csv files and concatenates them into a single file
"""
from etl.csv_concat import CSVConcat

if __name__ == "__main__":
    csv_concat = CSVConcat()
    csv_concat.run()
