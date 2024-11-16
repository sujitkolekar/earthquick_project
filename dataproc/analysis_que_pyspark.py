from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *
import configuration as conf


if __name__ == '__main__':
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

    # print(df.show())


    # 1. Count the number of earthquakes by region
    #
    # earthquick_count=df.groupby('area').agg(count('*').alias('Count_of_earthquicks_by_reagion'))
    # print(earthquick_count.show())

    # 2. Find the average magnitude by region

    # avg_magnitude=df.groupby('area').agg(avg('mag').alias('Average_Mag'))
    # print(avg_magnitude.show())

    # 3. Find how many earthquakes happen on the same day
    #
    # count_earthquakes_same_day=df.groupby(substring(str('time'),0,10)).agg(count('*').alias('Total_count_of_earthquakes'))
    # print(count_earthquakes_same_day.show())

    # 4. Find how many earthquakes happen on the same day and in the same region

    # samedayandsameregion=df.groupby(substring(str('time'),0,10),'area').agg(count('*').alias('Same_day_same_region_count'))
    # print(samedayandsameregion.show())

    # 5 Find average earthquakes happen on the same day

    # avg_earthqueick_same_day=df.groupby(substring(str('time'),0,10)).agg(count('*').alias('count_of_earthquicks')).agg(avg('count_of_earthquicks').alias('Avg_earthquick_same_day'))
    # print(avg_earthqueick_same_day.show())

    # 6.Find average earthquakes happen on the same day and in the same region

    # avg_earthqueick_sameday_sameregion=df.groupby(substring(str('time'),0,10),'area').agg(count('*').alias('Count_of_earthquicks')).agg(avg('Count_of_earthquicks').alias('Avg_of_earthquicks'))
    # print(avg_earthqueick_sameday_sameregion.show())

    # 7. Find the region name, which had the highest magnitude earthquake last week
    #
    # timestamp_to_date=df.select(to_date(substring(str('time'),0,10),'yyyy-mm-dd').alias('date'),'area','mag')
    # last_week=timestamp_to_date.filter(col('date')>=date_sub(current_date(),7))
    # heighst_mag=last_week.agg(max('mag'))
    # print(heighst_mag.show())

    # 8. Find the region name, which is having magnitudes higher than 5

    # region_name_of_mag=df.filter(col('mag')>5).select('area','mag')
    # print(region_name_of_mag.show())

    # 9. Find out the regions which are having the highest frequency and intensity of earthquakes

    # frequency_intensity = df.groupBy("area") \
    #         .agg(count("*").alias("highest_frequency"),avg("mag").alias("highest_intensity")) \
    # .orderBy(col("highest_frequency").desc(), col("highest_intensity").desc())
    #
    # print(frequency_intensity.show())




