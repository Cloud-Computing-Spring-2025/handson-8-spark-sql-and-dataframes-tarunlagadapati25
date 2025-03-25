from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count, avg, desc, sum, when, trim

spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# TODO: Implement the task here
# Positive (> 0.3), Neutral (-0.3 to 0.3), Negative (< -0.3)


# Save result
sentiment_df = posts_df.withColumn(
    "Sentiment", 
    when(col("SentimentScore") > 0.2, "Positive")
    .when(col("SentimentScore") < -0.2, "Negative")
    .otherwise("Neutral")
)
sentiment_engagement = sentiment_df.groupBy("Sentiment").agg(
    avg("Likes").alias("Avg Likes"), 
    avg("Retweets").alias("Avg Retweets")
)
sentiment_engagement.write.csv("output/sentiment_vs_engagement", header=True)