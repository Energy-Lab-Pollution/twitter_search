# Twitter Search

The repository contains the code for searching users on Twitter based on a given type of account, searches for tweets relating to the type, gets the users, tweets, and then if needed, increases the number of users by collecting all the lists that the users are a part of. We call this latter process, 'snowballing'.  The script can be run with or without the snowballing component. 

The processing and filtering scripts geo-locate the users, classify them based on relevance using a zero-shot model.


## How to Use

For the extraction part:
   
1. Clone the repository

```
git clone https://github.com/Energy-Lab-Pollution/twitter_search.git
```

2. Navigate to the repository
   
```   
cd ./twitter_search
```


3. Download Poetry, which allows the user to run the application in a virtual environment, [following these instructions](https://python-poetry.org/docs/). Then install poetry.
   
```
poetry install
```

4. Activate the virtual environment in poetry.

```
poetry shell
```
4.1. Receive the secret code from the authors, go to twitter_search/config_utils, and create a file called config.py. And paste all the secret codes there. 

5. Execute the project from the command line:

```bash
python3 twitter_search location(str) industry_type(str) list_needed(bool) --num_interations (int)
```


For example:  

```bash
python3 twitter_search Kolkata media False --num_iterations 1
```

gets users from kolkatta in the media industry, where lists(snowballing) is not needed. The optional argument number of iterations tells the script how many snowballing iterations are needed. Each iteration fetches lists, gets all users from the lists and filters the users based on location and content relevance.  

6 . The project will search Twitter based on the specified query and location, collecting user data and saving it in the raw data directory.

7. If you want, it is also possible to generate csv files for a particular location. The command for generating Kolkata's csv files  would be:

```bash
# Go to the etl directory
cd twitter_search/etl

# Run the script
python3 generate_csv_files.py "Kolkata"
```





