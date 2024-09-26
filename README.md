# CMPT 353 Project - Reddit Data Analysis

## Investigating the relationship between the sentiment of a reddit submission and it's popularity, as well as the correlation of other attributes. 

## Running the Code
We used the following subreddits for our analysis: 

  “r/science”, “r/technology”, “r/programming”, “r/antiwork”, and “r/aznidentity”. 
  
However, you may specify the specific subreddits you want to use in Step 1.


### 1. Load the submissions and comments data
**Python script:** `1.1_get_posts.py`
- **Description:** This script loads the 500 newest submissions from the given subreddit(s) and saves the submissions dataframe as compressed json files under 'reddit-data/submissions/', overwriting the previous contents.
- **Instructions:** Run `1.1_get_posts.py` and provide the subreddits you want in the command line arguments (ex. python3 get_reddit_posts.py antiwork science).

**Python script:** `1.2_get_comments.py`
- **Description:** This script loads the 50 newest submissions from the given subreddit(s), along with their respective (top-level) comments. It saves the submissions dataframe under 'reddit-data/comments/subs' and the comments dataframe under 'reddit-data/comments/comms', overwriting the previous contents. Note that the subreddits used here should be the same ones used during get_reddit_posts.py.
- **Instructions:** Run `1.2_get_comments.py` and provide the subreddits you want in the command line arguments.


### 2. Clean the submissions and comments data
- **Notebook:** `2.1_clean_posts.ipynb`
- **Description:** This notebook loads the reddit submissions from reddit-data, adds necessary columns and removes invalid or unneeded data, then saves as a csv file under 'submissions_cleaned/', overwriting the previous contents.
- **Instructions:** Open `2.1_clean_posts.ipynb` in Jupyter Notebook and run all cells from start to finish. 

- **Notebook:** `2.2_clean_comments.ipynb`
- **Description:** This notebook loads the reddit comments and their respective submissions from reddit-data, adds necessary columns and removes invalid or unneeded data, then saves as a csv file under 'comments_cleaned/', overwriting the previous contents.
- **Instructions:** Open `2.2_clean_comments.ipynb` in Jupyter Notebook and run all cells from start to finish. 


### 3. Run stats tests 

- **Notebook:** `3_stats_tests.ipynb`
- **Description:** This notebook does different stats tests to analyze the relationship between several attributes in the submissions and comments dataframes.
- **Instructions:** Open `3_stats_tests.ipynb` in Jupyter Notebook and run all cells from start to finish


By: Grace Li and Simran Mann 
