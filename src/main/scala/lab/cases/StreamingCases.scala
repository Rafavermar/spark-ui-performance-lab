package lab.cases

import lab.utils.{LabPaths, StreamingSupport}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object StreamingCaseSupport {
  val BootstrapServers = "redpanda:9092"
  val InputTopic = "spark-ui-lab-input"
  val OutputTopic = "spark-ui-lab-output"
  val StatefulInputTopic = "spark-ui-lab-stateful-input"

  val JsonSchema: StructType = StructType(Seq(
    StructField("id", LongType, nullable = false),
    StructField("key", StringType, nullable = false),
    StructField("event_time", StringType, nullable = false),
    StructField("value", LongType, nullable = false)
  ))

  def kafkaJsonStream(spark: SparkSession, topic: String, maxOffsetsPerTrigger: Int): DataFrame = {
    import spark.implicits._
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger.toString)
      .load()
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json($"json", JsonSchema).as("data"))
      .select("data.*")
      .withColumn("event_ts", to_timestamp($"event_time"))
  }
}

object StructuredStreamingBacklogCase extends LabCase {
  override val id: String = "15_structured_streaming_backlog"
  override val description: String = "A streaming query processes input more slowly than data arrives."
  override val uiFocus: Seq[String] = Seq("Structured Streaming")

  private var query: Option[StreamingQuery] = None

  override def runBaseline(spark: SparkSession): Unit = {
    val input = StreamingCaseSupport.kafkaJsonStream(spark, StreamingCaseSupport.InputTopic, maxOffsetsPerTrigger = 500)
      .withColumn("processing_bucket", col("value") % 20)

    query = Some(StreamingSupport.remember(input.writeStream
      .queryName("case15_backlog_baseline")
      .option("checkpointLocation", LabPaths.checkpoint("case15_backlog_baseline"))
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        Thread.sleep(3500L)
        println(s"case15 baseline batch=$batchId rows=${batch.count()}")
      }
      .start()))
  }

  override def runOptimized(spark: SparkSession): Unit = {
    val input = StreamingCaseSupport.kafkaJsonStream(spark, StreamingCaseSupport.InputTopic, maxOffsetsPerTrigger = 150)
      .withColumn("processing_bucket", col("value") % 20)

    query = Some(StreamingSupport.remember(input.writeStream
      .queryName("case15_backlog_optimized")
      .option("checkpointLocation", LabPaths.checkpoint("case15_backlog_optimized"))
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        println(s"case15 optimized batch=$batchId rows=${batch.count()}")
      }
      .start()))
  }

  override def afterInspection(spark: SparkSession, mode: String): Unit = {
    StreamingSupport.stop(query)
    query = None
  }
}

object StatefulStreamingCase extends LabCase {
  override val id: String = "16_stateful_streaming"
  override val description: String = "A stateful streaming aggregation can grow state without a bounded watermark strategy."
  override val uiFocus: Seq[String] = Seq("Structured Streaming")

  private var query: Option[StreamingQuery] = None

  override def runBaseline(spark: SparkSession): Unit = {
    val parsed = StreamingCaseSupport.kafkaJsonStream(spark, StreamingCaseSupport.StatefulInputTopic, maxOffsetsPerTrigger = 300)
      .where(col("event_ts").isNotNull)

    val counts = parsed
      .groupBy(window(col("event_ts"), "10 minutes"), col("key"))
      .agg(count(lit(1)).as("rows"))

    query = Some(StreamingSupport.remember(counts.writeStream
      .queryName("case16_stateful_baseline")
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .option("numRows", "10")
      .option("checkpointLocation", LabPaths.checkpoint("case16_stateful_baseline"))
      .trigger(Trigger.ProcessingTime("4 seconds"))
      .start()))
  }

  override def runOptimized(spark: SparkSession): Unit = {
    val parsed = StreamingCaseSupport.kafkaJsonStream(spark, StreamingCaseSupport.StatefulInputTopic, maxOffsetsPerTrigger = 300)
      .where(col("event_ts").isNotNull)
      .withWatermark("event_ts", "1 minute")

    val counts = parsed
      .groupBy(window(col("event_ts"), "1 minute"), col("key"))
      .agg(count(lit(1)).as("rows"))

    query = Some(StreamingSupport.remember(counts.writeStream
      .queryName("case16_stateful_optimized")
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option("numRows", "10")
      .option("checkpointLocation", LabPaths.checkpoint("case16_stateful_optimized"))
      .trigger(Trigger.ProcessingTime("4 seconds"))
      .start()))
  }

  override def afterInspection(spark: SparkSession, mode: String): Unit = {
    StreamingSupport.stop(query)
    query = None
  }
}

object RealTimeModeCase extends LabCase {
  override val id: String = "17_real_time_mode"
  override val description: String = "A stateless streaming query compares standard micro-batch with Spark 4.1 real-time mode."
  override val uiFocus: Seq[String] = Seq("Structured Streaming")

  private var query: Option[StreamingQuery] = None

  override def run(spark: SparkSession, mode: String): Unit =
    mode match {
      case "baseline" => runBaseline(spark)
      case "optimized" | "advanced" => runOptimized(spark)
      case other => throw new IllegalArgumentException(s"Unsupported mode '$other' for $id. Use baseline, optimized or advanced.")
    }

  override def runBaseline(spark: SparkSession): Unit = {
    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", StreamingCaseSupport.BootstrapServers)
      .option("subscribe", StreamingCaseSupport.InputTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .select(col("key"), col("value"))

    query = Some(StreamingSupport.remember(stream.writeStream
      .queryName("case17_micro_batch_baseline")
      .format("kafka")
      .option("kafka.bootstrap.servers", StreamingCaseSupport.BootstrapServers)
      .option("topic", StreamingCaseSupport.OutputTopic)
      .option("checkpointLocation", LabPaths.checkpoint("case17_micro_batch_baseline"))
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()))
  }

  override def runOptimized(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.streaming.realTimeMode.minBatchDuration", "5s")

    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", StreamingCaseSupport.BootstrapServers)
      .option("subscribe", StreamingCaseSupport.InputTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .select(col("key"), col("value"))

    query = Some(StreamingSupport.remember(stream.writeStream
      .queryName("case17_real_time_advanced")
      .format("kafka")
      .option("kafka.bootstrap.servers", StreamingCaseSupport.BootstrapServers)
      .option("topic", StreamingCaseSupport.OutputTopic)
      .option("checkpointLocation", LabPaths.checkpoint("case17_real_time_advanced"))
      .outputMode(OutputMode.Update())
      .trigger(Trigger.RealTime("5 seconds"))
      .start()))
  }

  override def afterInspection(spark: SparkSession, mode: String): Unit = {
    StreamingSupport.stop(query)
    query = None
  }
}
