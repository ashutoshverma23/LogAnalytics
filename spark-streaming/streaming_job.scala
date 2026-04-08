import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object RealTimeLogProcessor {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RealTimeLogProcessor")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Define schema for incoming JSON logs
    val logSchema = new StructType()
      .add("timestamp", StringType)
      .add("level", StringType)
      .add("ip", StringType)
      .add("url", StringType)
      .add("user_agent", StringType)
      .add("latency_ms", IntegerType)

    // Read streaming data from Kafka
    val rawLogs = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "logs_topic")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as jsonStr")

    // Parse JSON log entries
    val logs = rawLogs.select(from_json(col("jsonStr"), logSchema).alias("data"))
      .select("data.*")

    // Example transformation: Count logs by log level
    val levelCounts = logs
      .groupBy("level")
      .count()

    // Write processed results to Elasticsearch
    val query = levelCounts.writeStream
      .outputMode("complete")
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", "/tmp/checkpoints")
      .option("es.nodes", "elasticsearch")
      .option("es.port", "9200")
      .option("es.resource", "logs-index")
      .start()

    query.awaitTermination()
  }
}