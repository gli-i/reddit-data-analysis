# CMPT 353 Project - Reddit Data Analysis

## Investigating the relationship between the sentiment of a reddit submission and it's popularity, as well as the correlation of other attributes. 

## Running the Code
We used the following subreddits for our analysis: 

  “r/science”, “r/technology”, “r/programming”, “r/antiwork”, and “r/aznidentity”. 
  
If you want to use your own subreddit data, make sure you have some reddit data loaded from the cluster and stored in a folder named 'reddit-data'. https://coursys.sfu.ca/2024su-cmpt-353-d1/pages/RedditData. 

Follow the steps below to use our reddit data: 


### 1.  Run stats tests 

- **Notebook:** `stats_tests.ipynb`
- **Description:** This notebook does different stats tests to analyze the relationship between several attributes in the submissions and comments dataframes.
- **Instructions:** Open `stats_tests.ipynb` in Jupyter Notebook and run all cells from start to finish


Alternativley, if you want to load your own data from a different set of subreddits: 


### 1. Load the submissions data and do sentiment analysis 

- **Notebook:** `clean_reddit_posts.ipynb`
- **Description:** This notebook loads the reddit submissions from reddit-data and performs sentiment analysis as well as adds necessary columns to the submissions dataframe, then saves as a csv file under 'submissions_cleaned/'. 
- **Instructions:** Open `clean_reddit_posts.ipynb` in Jupyter Notebook and run all cells from start to finsih. 

### 2. Load the comments data: run get_reddit_comments.py and provide the list of subreddits you are choosing 
- **Python script:** `get_reddit_comments.py`
- **Description:** This script loads commetns from the given subreddits and links them to their respective submissions and saves the comments and submissions dataframes under 'comments_cleaned/'. The subset of submissions in this folder has additional attributes, representing the number of positive, negative and netural comments under each post.
- **Instructions:** Run `get_reddit_comments.py` and provide the subreddits you want in the command line arguments 

### 3. Run stats tests 

- **Notebook:** `stats_tests.ipynb`
- **Description:** This notebook does different stats tests to analyze the relationship between several attributes in the submissions and comments dataframes.
- **Instructions:** Open `stats_tests.ipynb` in Jupyter Notebook and run all cells from start to finish


By: Grace Li and Simran Mann 
