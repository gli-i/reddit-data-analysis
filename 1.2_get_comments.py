import sys
import praw
import config
from pyspark.sql import SparkSession, types
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import isnan

def main():
    # info for the reddit instance
    reddit = praw.Reddit(
        client_id=config.client_id,
        client_secret=config.client_secret,
        user_agent=config.user_agent,
        password=config.password,
        username=config.username,
    )

    subs_schema = types.StructType([
        types.StructField('id', types.StringType()),
        types.StructField('subreddit', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('selftext', types.StringType()),
        types.StructField('score', types.LongType()),
        types.StructField('upvote_ratio', types.FloatType()),
        types.StructField('num_comments', types.LongType()),
        types.StructField('date_created', types.FloatType()),
    ])
    comms_schema = types.StructType([
        types.StructField('link_id', types.StringType()),
        types.StructField('body', types.StringType()),
        types.StructField('score', types.LongType()),
        types.StructField('date_created', types.FloatType())
    ])

    overwrite_subs = True
    overwrite_comms = True

    for subreddit_name in subreddit_names:
        subreddit = reddit.subreddit(subreddit_name)

        # create lists to store rows of submission & comment data
        submissions_list = []
        comments_list = []

        # iterate through new submissions in the subreddit
        for submission in subreddit.new(limit=50):
            submissions_list.append(
                [submission.id, submission.subreddit.display_name, submission.title, submission.selftext, submission.score, 
                 submission.upvote_ratio, submission.num_comments, submission.created_utc]
            )

            # iterate through comments in that submission
            submission.comments.replace_more(limit=0)
            for comment in list(submission.comments):
                # if comment.author == "AutoModerator":   # r/science has this bot that automatically comments on every post
                #     continue

                if comment.body == "[removed]": # if the post was removed, ignore it
                    continue

                comments_list.append(
                    [comment.link_id, (comment.body)[:800], comment.score, comment.created_utc] # really long posts are truncated to 800 chars
                )

        subs_df = spark.createDataFrame(submissions_list, schema=subs_schema)
        comm_df = spark.createDataFrame(comments_list, schema=comms_schema)

        # time must be converted from unix time to timestamp
        subs_df = subs_df.withColumn("date_created", subs_df["date_created"].cast(TimestampType()))
        comm_df = comm_df.withColumn("date_created", comm_df["date_created"].cast(TimestampType()))

        comm_df = comm_df.repartition(8)

        if overwrite_subs:
            subs_df.write.json("reddit-data/comments/subs", compression='gzip', mode='overwrite')
        else:
            subs_df.write.json("reddit-data/comments/subs", compression='gzip', mode='append')

        while (comm_df.isEmpty() == False):
            limited_comm = comm_df.limit(100)
            comm_df = comm_df.subtract(limited_comm)

            if overwrite_comms: # only overwrite previous data the first time
                limited_comm.write.json("reddit-data/comments/comms", compression='gzip', mode='overwrite')
                overwrite_comms = False
            else:
                limited_comm.write.json("reddit-data/comments/comms", compression='gzip', mode='append')


if __name__=='__main__':
    spark = SparkSession.builder.appName('reddit-submissions-getter').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
    assert spark.version >= '3.2' # make sure we have Spark 3.2+

    subreddit_names = sys.argv[1:]
    main()