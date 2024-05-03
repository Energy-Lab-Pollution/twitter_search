# Twitter Search

The repository contains the code for searching users on twitter based on a given query, and it then applies several filters to keep the users relevant to us. 

## File Descriptions

For the extraction part, the following files are used:

- **\_\_main\_\_.py:** Entry point of the project. Executes the main functionality based on command-line arguments.

- **config_utils/:** Contains utility functions related to project configuration, such as creating a Tweepy client, defining constants, etc.

- **data/:** Directory for storing collected data. Includes subdirectories for cleaned and raw data.

- **etl/:** Stands for Extract, Transform, Load. This directory contains modules for data cleaning, data collection, and running Twitter searches.

  - **data_cleaning/:** Module for cleaning user data.

  - **data_collection/:** Module for collecting data, including functions for searching tweets and users.

  - **run_search_twitter.py:** Script for executing Twitter searches

  - **generate_csv_files.py:** Script for generating CSV files from the collected data, specifying a location.

- **run.py:** Module containing the main functionality for searching Twitter and saving user data.

For the filtering part, the following files are used, which are located in the `twitter_filtering` directory:

- **lists_filtering/** Directory with code for filtering the retrieved lists.

- **users_filtering/** Directory with code for filtering the retrieved users.

- **utils/** Directory with utility functions and constants for filtering.

- **__main__.py:** Entry point of the filtering part of the project.

## How to Use

For the extraction part:

1. Ensure you have the required dependencies installed by using the provided `poetry.lock` and `pyproject.toml` files.

2. Execute the project using the `__main__.py` file. Provide necessary command-line arguments like location and algorithm.

3. The project will search Twitter based on the specified query and location, collecting user data and saving it in the raw data directory.

For the filtering part:

Go to the `twitter_filtering` directory and execute the `__main__.py` file. The script will filter the users (still not implemented in the file) and lists based on the specified criteria.

