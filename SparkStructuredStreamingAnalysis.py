'''
Author: Owden Godson
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sqrt

# Create a Spark session
spark = SparkSession.builder.appName("StructuredStreamingAnalysis").getOrCreate()

# Read JSON files into a Streaming DataFrame
json_folder_path = "activity-data/*.json"
activity_stream_df = (
    spark.readStream.format("json")
    .schema(
        "Arrival_Time timestamp, Creation_Time timestamp, Device string, Index int, Model string, User string, gt string, x double, y double, z double")
    .option("maxFilesPerTrigger", 5)  # Setting maxFilesPerTrigger
    .load(json_folder_path)
)

# Define the 15-minute window duration
window_duration = "15 minutes"

# Group by 'gt' and calculate count and mean of 'x', 'y', 'z' for the 15-minute window
query = (
    activity_stream_df
    .withWatermark("Arrival_Time", "10 seconds")  # Setting watermark for handling late data
    .groupBy(
        window("Creation_Time", window_duration),  # Applying window duration
        col("gt")
    )
    .agg(
        {"x": "mean", "y": "mean", "z": "mean", "gt": "count"}  # Calculating mean of x, y, z and count of gt
    )
    .withColumnRenamed("avg(x)", "mean_x")
    .withColumnRenamed("avg(y)", "mean_y")
    .withColumnRenamed("avg(z)", "mean_z")
    .withColumnRenamed("count(gt)", "gt_count")
    .orderBy("window")
)

# Output to console and await termination for 30 seconds for each window duration
query.writeStream.outputMode("complete").format("console").start().awaitTermination(30)

# Perform additional analysis based on the calculated values
result_df = query.toPandas()

# Save the results back to Spark as a table
result_spark_df = spark.createDataFrame(result_df)  # Convert Pandas DataFrame to Spark DataFrame
result_spark_df.write.mode("overwrite").saveAsTable("result_table")  # Save as a Spark table

# Check for recommendations
for i in range(len(result_df) - 1):
    # Check if "sit" count is more than "stand" count
    if result_df.iloc[i]['gt'] == 'sit' and result_df.iloc[i]['gt_count'] > result_df.iloc[i]['gt_count']:
        print("Standing recommended")

    # Calculate distance between consecutive windows for 'move' recommendation
    if i >= 2:
        distance_1 = sqrt(
            (result_df.iloc[i]['mean_x'] - result_df.iloc[i - 1]['mean_x']) ** 2 +
            (result_df.iloc[i]['mean_y'] - result_df.iloc[i - 1]['mean_y']) ** 2 +
            (result_df.iloc[i]['mean_z'] - result_df.iloc[i - 1]['mean_z']) ** 2
        )
        distance_2 = sqrt(
            (result_df.iloc[i - 1]['mean_x'] - result_df.iloc[i - 2]['mean_x']) ** 2 +
            (result_df.iloc[i - 1]['mean_y'] - result_df.iloc[i - 2]['mean_y']) ** 2 +
            (result_df.iloc[i - 1]['mean_z'] - result_df.iloc[i - 2]['mean_z']) ** 2
        )

        # Print 'move' recommendation based on average distance comparison
        if distance_1 < distance_2:
            print("Move recommended")

spark.stop()
