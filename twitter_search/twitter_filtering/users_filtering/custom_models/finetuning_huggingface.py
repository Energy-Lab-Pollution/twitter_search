"""
Adding script to finetune the HF NLP model
"""
import pandas as pd

from pathlib import Path

script_path = Path(__file__).resolve()
project_root = script_path.parents[3]
CLEAN_DATA_PATH = project_root / "data" / "labeled_data"


def labeled_data_to_csv():
    """
    Converts excel file to csv file
    """

    labeled_data = pd.read_excel(f"{CLEAN_DATA_PATH}/users_to_label.xlsx",
                                 sheet_name="Random Sample")
    labeled_data.to_csv(f"{CLEAN_DATA_PATH}/users_to_label.csv", index=False)
    return labeled_data


labeled_data = labeled_data_to_csv()
print(labeled_data)
