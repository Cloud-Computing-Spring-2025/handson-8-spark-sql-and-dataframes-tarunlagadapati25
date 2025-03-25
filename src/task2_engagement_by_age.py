from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count, avg, desc, sum, when, trim

spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# TODO: Implement the task here


# Save result
engagement_df = posts_df.join(users_df, "UserID")
age_engagement = engagement_df.groupBy("AgeGroup").agg(
    avg("Likes").alias("Avg Likes"), 
    avg("Retweets").alias("Avg Retweets")
)
ranked_engagement = age_engagement.orderBy(desc("Avg Likes"))
ranked_engagement.write.csv("output/engagement_by_age", header=True)

