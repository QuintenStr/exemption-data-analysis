
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os

# Create a Spark session
spark = SparkSession.builder.appName("CommentClassification").getOrCreate()

# Load the trained model from the ML pipeline
model_path = '/Oefeningen/Project/bestmodel'
model = PipelineModel.load(model_path)

# Define the input directory where comments are stored
input_directory = '/Oefeningen/Project/Comments/'

# Set the streaming interval to 30 seconds
streaming_interval = "30 seconds"

# Define a schema for the JSON data
schema = StructType().add("id", StringType()).add("author", StringType()).add("tweet", StringType()).add("replies", IntegerType()).add("score", IntegerType()).add("submission_comment_count", IntegerType()).add("submission_score", IntegerType()).add("timestamp", DoubleType ())

# Create a streaming DataFrame by monitoring the input directory
comments_df = spark.readStream.schema(schema).json(input_directory)

# Apply the model to classify each comment
classified_df = model.transform(comments_df)

# Select the required columns
result_df = classified_df.select("id", "author", "tweet", "replies", "score", "submission_comment_count", "submission_score", "timestamp", "prediction")

# Define the output directory for the JSON file
output_directory = './'

# Define the JSON file name
json_file_name = 'result.json'

# Define a function to write rows as JSON objects to a file
def write_json_file(rows):
    # Create an empty list to store JSON objects
    json_objects = []
    
    # Iterate over each row and convert it to a JSON object
    for row in rows:
        json_objects.append(row.asDict())
    
    # Define the file path for the JSON file
    json_file_path = os.path.join(output_directory, json_file_name)
    
    # Check if the JSON file exists
    if os.path.exists(json_file_path):
        # Read the existing contents of the file
        with open(json_file_path, 'r') as json_file:
            existing_json_objects = json_file.readlines()
        
        # Append the new JSON objects to the existing ones
        json_objects = existing_json_objects + [json.dumps(json_object) + '\n' for json_object in json_objects]
    
    # Write the JSON objects to the file
    with open(json_file_path, 'w') as json_file:
        for json_object in json_objects:
            json_file.write(json_object)

# Define a function to process each batch
def process_batch(batch_df, batch_id):
    # Count the number of predictions for class 0 and class 1
    class_0_count = batch_df.filter(batch_df.prediction == 0).count()
    class_1_count = batch_df.filter(batch_df.prediction == 1).count()
    
    # Create a DataFrame with class and count information
    count_df = spark.createDataFrame([(batch_id, class_0_count, class_1_count)], ["BatchID", "Class0Count", "Class1Count"])
    
    # Print the DataFrame
    count_df.show()
    
    # Input moest 'tweet' kolom hebben als text voor prediction maar nu renamen we die naar 'text'
    batch_df = batch_df.withColumnRenamed("tweet", "text")
    batch_df.show()
   
    # Write the rows as JSON objects to the file
    write_json_file(batch_df.collect())


# Start the streaming query and process each batch
query = result_df.writeStream.outputMode("append").foreachBatch(process_batch).trigger(processingTime=streaming_interval).start()

# Wait for the streaming to finish
query.awaitTermination()
