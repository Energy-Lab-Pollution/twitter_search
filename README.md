
## File Descriptions

- **\_\_main\_\_.py:** Entry point of the project. Executes the main functionality based on command-line arguments.

- **config_utils/:** Contains utility functions related to project configuration, such as creating a Tweepy client.

- **data/:** Directory for storing collected data. Includes subdirectories for cleaned and raw data.

- **etl/:** Stands for Extract, Transform, Load. This directory contains modules for data cleaning, data collection, and running Twitter searches.

  - **data_cleaning/:** Module for cleaning user data.

  - **data_collection/:** Module for collecting data, including functions for searching tweets and users.

  - **run_search_twitter.py:** Script for executing Twitter searches.

- **run.py:** Module containing the main functionality for searching Twitter and saving user data.

## How to Use

1. Ensure you have the required dependencies installed by using the provided `poetry.lock` and `pyproject.toml` files.

2. Execute the project using the `__main__.py` file. Provide necessary command-line arguments like location and algorithm.

3. The project will search Twitter based on the specified query and location, collecting user data and saving it in the raw data directory.


