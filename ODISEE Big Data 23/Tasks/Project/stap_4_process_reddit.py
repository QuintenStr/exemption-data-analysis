
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

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
schema = StructType().add("id", StringType()).add("tweet", StringType())

# Create a streaming DataFrame by monitoring the input directory
comments_df = spark.readStream.schema(schema).json(input_directory)

# Apply the model to classify each comment
classified_df = model.transform(comments_df)

# Select the required columns
result_df = classified_df.select("id", "tweet", "prediction")

# Define a function to process each batch
def process_batch(batch_df, batch_id):
    # Count the number of predictions for class 0 and class 1
    class_0_count = batch_df.filter(batch_df.prediction == 0).count()
    class_1_count = batch_df.filter(batch_df.prediction == 1).count()
    
    # Create a DataFrame with class and count information
    count_df = spark.createDataFrame([(batch_id, class_0_count, class_1_count)], ["BatchID", "Class0Count", "Class1Count"])
    
    # Print the DataFrame
    count_df.show()

# Start the streaming query and process each batch
query = result_df.writeStream.outputMode("append").foreachBatch(process_batch).trigger(processingTime=streaming_interval).start()

# Wait for the streaming to finish
query.awaitTermination()
