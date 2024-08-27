import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.classification import GBTClassifier
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

start_time = time.time()

spark = SparkSession.builder.config("spark.driver.memory", "8g").config('spark.executor.memory', '4g').appName("Project").getOrCreate()

# df inlezen als sparkdf van csv
spark_df = spark.read.option("header",True).csv("/Oefeningen/Project/cleaned.csv")

# 20 null rijen droppen en van multiclass naar binary
spark_df = spark_df.filter(spark_df.tweet.isNotNull())
spark_df = spark_df.withColumn("class", col("class").cast(IntegerType()))
spark_df = spark_df.filter((col("class") == 1) | (col("class") == 2))
spark_df = spark_df.withColumn("class", when(col("class") == 2, 0).otherwise(col("class")))
# nu is 0 = neutral en 1 = offensive

# train test split
train_data, test_data = spark_df.select("tweet", "class").randomSplit([0.7, 0.3], seed=420)

# pipeline
tokenizer = RegexTokenizer(inputCol="tweet", outputCol="words", pattern="\\W")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features")
idf = IDF(inputCol="raw_features", outputCol="features")
gbt = GBTClassifier(labelCol="class", featuresCol="features")

# create pipeline
pipeline = Pipeline(stages=[tokenizer, remover, cv, idf, gbt])

# create grid of hyperparameters for grid search
paramGrid = ParamGridBuilder() \
    .addGrid(gbt.maxDepth, [2, 4, 6]) \
    .addGrid(gbt.maxBins, [20, 30, 40]) \
    .addGrid(gbt.maxIter, [10, 20, 30]) \
    .addGrid(gbt.stepSize, [0.1, 0.01]) \
    .build()

# create evaluator
evaluator = BinaryClassificationEvaluator(labelCol="class", rawPredictionCol="rawPrediction", metricName="areaUnderROC")

# create cross validator with 5-fold cross validation
cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)

# fit the model
model = cv.fit(train_data)

end_time = time.time()
print("Total training time: {:.2f} seconds".format(end_time - start_time))
best_pipeline_model = model.bestModel
best_pipeline_model.save('/Oefeningen/Project/bestmodel')
spark.stop()
