from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("SparkKafkaToMinIO")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("level", StringType()),
    StructField("ip", StringType()),
    StructField("url", StringType()),
    StructField("user_agent", StringType()),
    StructField("latency_ms", IntegerType())
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "logs_topic")
    .load()
    .selectExpr("CAST(value AS STRING)")
)

logs = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

query = (
    logs.writeStream
    .format("parquet")
    .option("path", "s3a://processed-logs/")
    .option("checkpointLocation", "/tmp/checkpoints-s3")
    .start()
)

query.awaitTermination()