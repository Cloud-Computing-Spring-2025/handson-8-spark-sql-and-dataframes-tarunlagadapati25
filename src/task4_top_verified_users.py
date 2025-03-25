from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count, avg, desc, sum, when, trim

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# TODO: Implement the task here

# Save result
verified_users = users_df.filter(col("Verified") == True)
reach_df = posts_df.groupBy("UserID").agg(sum(col("Likes") + col("Retweets")).alias("Total Reach"))
verified_reach = verified_users.join(reach_df, "UserID").select("Username", "Total Reach")
top_verified = verified_reach.orderBy(desc("Total Reach")).limit(5)
top_verified.write.csv("output/top_verified_users", header=True)

