# Twitter Search

The repository contains the code for searching users on Twitter based on a given type of account, searches for tweets relating to the type, gets the users, tweets, and then if needed, increases the number of users by collecting all the lists that the users are a part of. We call this latter process, 'snowballing'.  The script can be run with or without the snowballing component. 

The processing and filtering scripts geo-locate the users, classify them based on relevance using a zero-shot model.


## How to Use

For the extraction part:

1. Ensure you have the required dependencies installed.

2. Execute the project from the command line: 
For example:  "python3 twitter_search Kolkata media False --num_iterations 1" gets users from kolkatta in the media industry, where lists(snowballing) is not needed. The optional argument number of iterations tells the script how many snowballing iterations are needed. Each iteration fetches lists, gets all users from the lists and filters the users based on location and content relevance.  

3. The project will search Twitter based on the specified query and location, collecting user data and saving it in the raw data directory.



