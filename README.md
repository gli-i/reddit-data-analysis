# CMPT 353 Project - Reddit Sentiment and Popularity Analysis

## Investigating the relationship between the sentiment of a reddit submission and it's popularity, defined by different factors

## Running the Code

First, make sure you have some reddit data loaded from the cluster and stored in a folder named 'reddit-subset'. https://coursys.sfu.ca/2024su-cmpt-353-d1/pages/RedditData

### 1. Load the data and do sentiment analysis 

- **Notebook:** `load_reddit_data.ipynb`
- **Description:** This notebook loads the reddit data from reddit-subset and performs sentiment analysis on comments and submissions. This can take a few minutes! 
- **Instructions:** Open `load_reddit_data.ipynb` in Jupyter Notebook and run all cells from start to finsih. 

### 2. Link comments to submissions

- **Notebook:** `link_comments_submissions_ipynb`
- **Description:** This notebook links comments to the submission it was posted on and counts the number of positive/negative/neutral comments for each post. It also calculates a popularity score for each submissinon, based on the number of positive/neutral/negative comments. 
- **Instructions:** Open `link_comments_submissions_ipynb` in Jupyter Notebook and run all cells from start to finish. 

### 3. Run stats tests 

- **Notebook:** `stats_tests.ipynb`
- **Description:** This notebook does different stats tests to analyze the relationship between sentiment and popularity of a submissions.
- **Instructions:** Open `stats_tests.ipynb` in Jupyter Notebook and run all cells from start to finish


By: Grace Li and Simran Mann 
