from pyspark.ml.classification import GBTClassificationModel
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, DoubleType, StructField
from pyspark.sql.functions import lit
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'


spark = SparkSession.builder \
    .appName("Kafka Stream Processing") \
    .getOrCreate()

schema = StructType([
    StructField("HighBP", DoubleType()),
    StructField("HighChol", DoubleType()),
    StructField("CholCheck", DoubleType()),
    StructField("BMI", DoubleType()),
    StructField("Smoker", DoubleType()),
    StructField("Stroke", DoubleType()),
    StructField("HeartDiseaseorAttack", DoubleType()),
    StructField("PhysActivity", DoubleType()),
    StructField("Fruits", DoubleType()),
    StructField("Veggies", DoubleType()),
    StructField("HvyAlcoholConsump", DoubleType()),
    StructField("AnyHealthcare", DoubleType()),
    StructField("NoDocbcCost", DoubleType()),
    StructField("GenHlth", DoubleType()),
    StructField("MentHlth", DoubleType()),
    StructField("PhysHlth", DoubleType()),
    StructField("DiffWalk", DoubleType()),
    StructField("Sex", DoubleType()),
    StructField("Age", DoubleType()),
    StructField("Education", DoubleType()),
    StructField("Income", DoubleType())
])

streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "health_data") \
    .load()

parsed_streaming_df = streaming_df \
    .select(from_json(col("value").cast("string"), schema).alias("json")) \
    .select("json.*")

assembler = VectorAssembler(
    inputCols=schema.names,
    outputCol="features"
)
parsed_streaming_df_transformed = assembler.transform(parsed_streaming_df)

loaded_model = GBTClassificationModel.load("gradient_boosting_model")

predicted_df = loaded_model.transform(parsed_streaming_df_transformed)

query = predicted_df \
    .withColumn("key", lit("constant_key_value")) \
    .selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "health_data_predicted") \
    .option("checkpointLocation", "/checkpoints") \
    .start()
query.awaitTermination()

spark.stop()
