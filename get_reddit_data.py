import sys
import praw
from pyspark.sql import SparkSession, types

def main():
    # info for the reddit instance
    reddit = praw.Reddit(
        client_id="gpyHj7yTFPBpeNY6Xxv_Tw",
        client_secret="uyJS3keAzErsZJ6t4DY7RMwe5P1JxA",
        user_agent="353 Sentiment Analysis by /u/PuzzleHeaded_Stay653",
        password="datascimorelikeateasci!!!",
        username="PuzzleHeaded_Stay653",
    )

    subs_schema = types.StructType([
        types.StructField('id', types.StringType()),
        types.StructField('subreddit', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('selftext', types.StringType()),
        types.StructField('score', types.LongType()),
        types.StructField('upvote_ratio', types.FloatType()),
        types.StructField('num_comments', types.LongType()),
    ])
    comms_schema = types.StructType([
        types.StructField('link_id', types.StringType()),
        types.StructField('body', types.StringType()),
        types.StructField('score', types.LongType()),
    ])

    overwrite = True

    for subreddit_name in subreddit_names:
        subreddit = reddit.subreddit(subreddit_name)

        # create lists to store rows of submission & comment data
        submissions_list = []
        comments_list = []

        # iterate through new submissions in the subreddit
        for submission in subreddit.new(limit=100):
            submissions_list.append([submission.id, submission.subreddit.display_name, submission.title, submission.selftext, submission.score, submission.upvote_ratio, submission.num_comments])

            # iterate through comments in that submission
            submission.comments.replace_more(limit=0)
            for comment in list(submission.comments):
                comments_list.append([comment.link_id, comment.body, comment.score])

        subs_df = spark.createDataFrame(submissions_list, schema=subs_schema)
        comm_df = spark.createDataFrame(comments_list, schema=comms_schema)

        if overwrite: # only overwrite previous data the first time
            subs_df.write.json("reddit-data/submissions", compression='gzip', mode='overwrite')
            comm_df.write.json("reddit-data/comments", compression='gzip', mode='overwrite')
            overwrite = False;
        else:
            subs_df.write.json("reddit-data/submissions", compression='gzip', mode='append')
            comm_df.write.json("reddit-data/comments", compression='gzip', mode='append')


if __name__=='__main__':
    spark = SparkSession.builder.appName('reddit-submissions-getter').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
    assert spark.version >= '3.2' # make sure we have Spark 3.2+

    subreddit_names = sys.argv[1:]
    main()