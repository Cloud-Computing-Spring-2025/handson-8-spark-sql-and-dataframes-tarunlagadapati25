from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count, avg, desc, sum, when, trim

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# TODO: Split the Hashtags column into individual hashtags and count the frequency of each hashtag and sort descending


# Save result
hashtags_df = posts_df.withColumn("Hashtag", explode(split(col("Hashtags"), ",")))
hashtag_counts = hashtags_df.groupBy("Hashtag").agg(count("Hashtag").alias("Count"))
top_hashtags = hashtag_counts.orderBy(desc("Count")).limit(10)
top_hashtags.write.csv("output/hashtag_trends", header=True)
