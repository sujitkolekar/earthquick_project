from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *
import configuration as conf
# Set the path to your service account credentials JSON file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = conf.GOOGLE_APPLICATION_CREDENTIALS

# Initialize SparkSession with BigQuery support
spark = SparkSession.builder \
    .appName("analysis_question") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.0") \
    .getOrCreate()

# Read the BigQuery table into a DataFrame
df = spark.read.format("bigquery") \
    .option("project", conf.PROJECT_ID) \
    .option("dataset", conf.DATASET_NAME) \
    .option("table", conf.TABLE_NAME) \
    .load()

# Show the DataFrame contents and schema
# print(df.show())  # Display the first few rows of the DataFrame
# print(df.printSchema())  # Print the schema of the DataFrame


#`delta-compass-440906.earthquake_project.dataproc_eartheqake`

df = df.withColumn("date", to_date(df["time"]))
# df.show()

    # # Analysis
    # # # 1. Count the number of earthquakes by region
# earthquake_count_by_region = df.groupBy("area").agg(count("*").alias("earthquake_count_by_region"))
# earthquake_count_by_region.show()
    #
    # # 2. Find the average magnitude by region
# average_magnitude_by_region = df.groupBy("area").agg(avg("mag").alias("average_magnitude_by_region"))
# average_magnitude_by_region.show()
    #
    # # # 3. Find how many earthquakes happen on the same day
# earthquakes_by_day = df.groupBy("date").agg(count("*").alias("earthquake_count_same_day"))
# earthquakes_by_day.show()
    #
    # # # 4. Find how many earthquakes happen on the same day and in the same region
# earthquakes_by_day_region = df.groupBy("date", "area").agg(count("*").alias("earthquake_count_same_day_same_region"))
# earthquakes_by_day_region.show()
    #
    # # # 5 Find average earthquakes happen on the same day
# average_earthquakes_per_day = df.groupBy("date").agg(count("*").alias("earthquake_count_per_day")).agg(avg("earthquake_count_per_day").alias("average_earthquakes_same_day"))
# average_earthquakes_per_day.show()
    #
    # # # 6.Find average earthquakes happen on the same day and in the same region
# average_earthquakes_per_day_region = df.groupBy("date", "area").agg(count("*").alias("earthquake_count"))
# average_earthquakes_per_day_region.show()
    #
    # # 7. Find the region name, which had the highest magnitude earthquake last week
# last_week = df.filter(col("date") >= date_sub(current_date(), 7))
# highest_magnitude_last_week = last_week.agg(max("mag").alias("highest_magnitude_last_week"))
# highest_magnitude_last_week.show()
    #
    # # # 8. Find the region name, which is having magnitudes higher than 5
# regions_high_magnitude = df.filter(col("mag") > 5).select("area").distinct()
# regions_high_magnitude.show()
    #
    # # # 9. Find out the regions which are having the highest frequency and intensity of earthquakes
# frequency_intensity = df.groupBy("area") \
#     .agg(count("*").alias("highest_frequency"),avg("mag").alias("highest_intensity")) \
#     .orderBy(col("highest_frequency").desc(), col("highest_intensity").desc())
#
# # Display the result
# frequency_intensity.show()