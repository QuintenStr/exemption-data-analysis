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

def process_batch(df, batch_id):
    print("Schema of incoming dataframe:")
    df.printSchema()

    print("Contents of incoming dataframe:")
    df.show(truncate=False)

    print("New dataframe with questions answered:")
    new_df = df.select("value")
    new_df = new_df.withColumn("value", col("value").cast("binary").cast("string"))
    new_df = new_df.withColumn("word_count", size(split(trim(col("value")), " ")))
    new_df = new_df.withColumn("char_count", length(col("value")))

    for letter in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ':
        new_df = new_df.withColumn(f"{letter.lower()}_lower_count", length(trim(expr(f"regexp_replace(value, '[^{letter.lower()}]', '')"))))

        new_df = new_df.withColumn(f"{letter.upper()}_upper_count", length(trim(expr(f"regexp_replace(value, '[^{letter.upper()}]', '')"))))
    
    new_df.show(truncate=False)
    
    print("Aantal rijen opgeteld:")
    print(new_df.count())


#new_df.show(truncate=False)

query = kafka_df.writeStream \
    .trigger(processingTime="20 seconds") \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
