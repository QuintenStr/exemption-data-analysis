# Kafka consumer met pyspark om een charcount uit te voeren

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, size, split, trim, length, regexp_replace, expr
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("KafkaStreamReader") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'localhost:9092') \
    .option("subscribe", "BookStream") \
    .option("startingOffsets", "earliest") \
    .load()

print("Schema of incoming dataframe:")
kafka_df.printSchema()

print("Contents of incoming dataframe:")

print("New dataframe with questions answered:")
new_df = kafka_df.select("value")
new_df = new_df.withColumn("value", col("value").cast("binary").cast("string"))
new_df = new_df.withColumn("word_count", size(split(trim(col("value")), " ")))
new_df = new_df.withColumn("char_count", length(col("value")))

for letter in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ':
    new_df = new_df.withColumn(f"{letter.lower()}_lower_count", length(trim(expr(f"regexp_replace(value, '[^{letter.lower()}]', '')"))))

    new_df = new_df.withColumn(f"{letter.upper()}_upper_count", length(trim(expr(f"regexp_replace(value, '[^{letter.upper()}]', '')"))))

query = new_df.writeStream \
    .option("truncate", "false") \
    .option("path", "~/bigdata/oefening-streaming-QuintenStr") \
    .format("console") \
    .trigger(processingTime="1 second") \
    .outputMode("append") \
    .start()

query.awaitTermination()
