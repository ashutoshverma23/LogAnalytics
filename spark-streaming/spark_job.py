from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

def main():
    spark = (
        SparkSession.builder
        .appName("RealTimeLogAnalytics")
        .master("local[*]")     # local run
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Schema for Kafka log messages
    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("level", StringType()),
        StructField("ip", StringType()),
        StructField("url", StringType()),
        StructField("user_agent", StringType()),
        StructField("latency_ms", IntegerType())
    ])

    # Read from Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "logs_topic")
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING)")
    )

    logs_df = (
        raw_df
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    # Simple aggregation
    agg_df = (
        logs_df
        .groupBy("level")
        .count()
    )

    # ✅ File sink (local testing)
    query = (
        agg_df.writeStream
        .outputMode("update")
        .format("parquet")
        .option("path", "./outputs/parquet")
        .option("checkpointLocation", "./checkpoints")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()