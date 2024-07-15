import sys
import praw
import pandas as pd
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit-submissions-getter').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+

# takes 1 argument - the name of the subreddit
def main():
    # info for the reddit instance
    reddit = praw.Reddit(
        client_id="gpyHj7yTFPBpeNY6Xxv_Tw",
        client_secret="uyJS3keAzErsZJ6t4DY7RMwe5P1JxA",
        user_agent="353 Sentiment Analysis by /u/PuzzleHeaded_Stay653",
        password="datascimorelikeateasci!!!",
        username="PuzzleHeaded_Stay653",
    )
    subreddit = reddit.subreddit(subreddit_name)
    
    # create lists to store rows of submission & comment data
    submissions_list = []
    comments_list = []

    # iterate through new submissions in the subreddit
    for submission in subreddit.new(limit=100):
        submissions_list.append([submission.id, submission.title, submission.selftext, submission.score, submission.num_comments])

        # iterate through comments in that submission
        submission.comments.replace_more(limit=0)
        for comment in list(submission.comments):
            comments_list.append([comment.link_id, comment.body, comment.score])

    subs_df = spark.createDataFrame(submissions_list, ["id", "title", "selftext", "score", "num_comments"])
    subs_df.show()

    # for submission in subreddit.new(limit=2):
    #     for comment in list(submission.comments):
    #         comments_list.append([comment.link_id, comment.body, comment.score])

    comm_df = spark.createDataFrame(comments_list, ["link_id", "body", "score"])
    comm_df.show()

    subs_df.write.json("reddit-data/submissions", compression='gzip', mode='overwrite')
    comm_df.write.json("reddit-data/comments", compression='gzip', mode='overwrite')

if __name__=='__main__':
    subreddit_name = sys.argv[1]
    main()