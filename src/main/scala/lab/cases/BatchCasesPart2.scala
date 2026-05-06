package lab.cases

import lab.data.SyntheticData
import lab.utils.{FsUtils, LabPaths, UiPrinter}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.util.Try

object TooManyPartitionsCase extends LabCase {
  override val id: String = "08_too_many_partitions"
  override val description: String = "Excessive partitions create many tiny tasks for small data."
  override val uiFocus: Seq[String] = Seq("Jobs", "Stages")

  override def runBaseline(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark.range(0, 30000, 1, 400)
      .repartition(400)
      .withColumn("bucket", ($"id" % 50).cast("int"))
      .withColumn("score", length(sha2($"id".cast("string"), 256)))
    UiPrinter.printBatchResult("partitions", df.rdd.getNumPartitions)
    UiPrinter.printBatchResult("groups", df.groupBy("bucket").agg(sum("score")).count())
  }

  override def runOptimized(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark.range(0, 30000, 1, 24)
      .coalesce(12)
      .withColumn("bucket", ($"id" % 50).cast("int"))
      .withColumn("score", length(sha2($"id".cast("string"), 256)))
    UiPrinter.printBatchResult("partitions", df.rdd.getNumPartitions)
    UiPrinter.printBatchResult("groups", df.groupBy("bucket").agg(sum("score")).count())
  }
}

object SpillCase extends LabCase {
  override val id: String = "09_spill"
  override val description: String = "Wide rows and low partition count can create memory pressure and spill."
  override val uiFocus: Seq[String] = Seq("Stages", "Executors")

  override def runBaseline(spark: SparkSession): Unit = {
    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    val wide = spark.range(0, 650000, 1, 8)
      .withColumn("agg_key", pmod(xxhash64($"id"), lit(220000)))
      .withColumn("payload", concat(
        sha2(concat_ws("-", $"id".cast("string"), lit("a")), 256),
        sha2(concat_ws("-", $"id".cast("string"), lit("b")), 256),
        sha2(concat_ws("-", $"id".cast("string"), lit("c")), 256)
      ))
      .repartition(4, $"agg_key")
      .sortWithinPartitions("payload")

    val result = wide.groupBy("agg_key").agg(max("payload").as("max_payload"), count(lit(1)).as("rows"))
    result.explain(extended = true)
    UiPrinter.printBatchResult("groups", result.count())
  }

  override def runOptimized(spark: SparkSession): Unit = {
    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.partitions", "24")
    val narrower = spark.range(0, 650000, 1, 24)
      .withColumn("agg_key", (pmod(xxhash64($"id"), lit(220000)) / 20).cast("long"))
      .withColumn("metric", ($"id" % 100).cast("long"))
      .repartition(24, $"agg_key")

    val result = narrower.groupBy("agg_key").agg(sum("metric").as("metric_sum"), count(lit(1)).as("rows"))
    result.explain(extended = true)
    UiPrinter.printBatchResult("groups", result.count())
  }
}

object CacheMisuseCase extends LabCase {
  override val id: String = "10_cache_misuse"
  override val description: String = "Caching data that is not reused consumes storage memory without benefit."
  override val uiFocus: Seq[String] = Seq("Storage", "Executors")

  private var cached: Option[DataFrame] = None

  override def runBaseline(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = SyntheticData.factFrame(spark, 180000)
      .withColumn("wide_payload", concat($"payload_a", $"payload_b", $"payload_c"))
      .persist(StorageLevel.MEMORY_AND_DISK)
    cached = Some(df)

    UiPrinter.printBatchResult("materialized cached rows", df.count())
    UiPrinter.printBatchResult("single downstream use", df.groupBy("country_id").agg(sum("amount")).count())
    println("The Storage tab should show cached data even though it was not reused enough to justify caching.")
  }

  override def runOptimized(spark: SparkSession): Unit = {
    val df = SyntheticData.factFrame(spark, 180000)
    UiPrinter.printBatchResult("single downstream use", df.groupBy("country_id").agg(sum("amount")).count())
    println("The Storage tab should remain empty or much quieter because no unnecessary cache is created.")
  }

  override def afterInspection(spark: SparkSession, mode: String): Unit = {
    cached.foreach(_.unpersist(blocking = false))
    cached = None
  }
}

object UdfCostCase extends LabCase {
  override val id: String = "11_udf_cost"
  override val description: String = "A Scala UDF hides simple logic from Catalyst where built-in SQL functions would work."
  override val uiFocus: Seq[String] = Seq("SQL")

  override def runBaseline(spark: SparkSession): Unit = {
    import spark.implicits._
    val labelUdf = udf((id: Long) => if (id % 2 == 0) "even" else "odd")
    val result = spark.range(0, 350000, 1, 24)
      .withColumn("label", labelUdf($"id"))
      .groupBy("label")
      .agg(count(lit(1)).as("rows"))

    result.explain(extended = true)
    result.collect().foreach(row => println(row.mkString(" | ")))
  }

  override def runOptimized(spark: SparkSession): Unit = {
    import spark.implicits._
    val result = spark.range(0, 350000, 1, 24)
      .withColumn("label", when($"id" % 2 === 0, lit("even")).otherwise(lit("odd")))
      .groupBy("label")
      .agg(count(lit(1)).as("rows"))

    result.explain(extended = true)
    result.collect().foreach(row => println(row.mkString(" | ")))
  }
}

object AqeComparisonCase extends LabCase {
  override val id: String = "12_aqe_comparison"
  override val description: String = "The same query is easier for Spark to adapt when Adaptive Query Execution is enabled."
  override val uiFocus: Seq[String] = Seq("SQL", "Stages")

  private def query(spark: SparkSession): DataFrame =
    SyntheticData.fact(spark)
      .join(SyntheticData.dim(spark), Seq("customer_id"))
      .groupBy("segment", "country_id")
      .agg(sum("amount").as("amount_sum"), count(lit(1)).as("rows"))
      .orderBy(desc("amount_sum"))

  override def runBaseline(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    val result = query(spark)
    result.explain(extended = true)
    UiPrinter.printBatchResult("rows", result.count())
  }

  override def runOptimized(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    val result = query(spark)
    result.explain(extended = true)
    UiPrinter.printBatchResult("rows", result.count())
  }
}

object TaskFailureRetryCase extends LabCase {
  override val id: String = "13_task_failure_retry"
  override val description: String = "A controlled transient task failure makes retries visible in Jobs and Stages."
  override val uiFocus: Seq[String] = Seq("Jobs", "Stages", "Executors")

  override def runBaseline(spark: SparkSession): Unit = {
    import spark.implicits._
    val marker = "/opt/spark-checkpoints/case13_fail_once.marker"
    FsUtils.delete(spark, marker)

    val rdd = spark.sparkContext.parallelize(0 until 80000, 8).mapPartitionsWithIndex { case (partitionId, rows) =>
      val file = new java.io.File(marker)
      if (partitionId == 3 && TaskContext.get().attemptNumber() == 0 && !file.exists()) {
        file.getParentFile.mkdirs()
        file.createNewFile()
        throw new RuntimeException("Intentional one-time failure for Spark UI retry evidence.")
      }
      rows.map(value => (value % 20, value.toLong))
    }

    val df = rdd.toDF("bucket", "value")
    UiPrinter.printBatchResult("groups after retry", df.groupBy("bucket").agg(sum("value")).count())
  }

  override def runOptimized(spark: SparkSession): Unit = {
    import spark.implicits._
    val raw = spark.range(0, 80000, 1, 8)
      .withColumn("raw_value", when($"id" === 31337L, lit("bad")).otherwise(($"id" % 1000).cast("string")))

    val cleaned = raw
      .where($"raw_value".rlike("^[0-9]+$"))
      .withColumn("value", $"raw_value".cast("long"))
      .withColumn("bucket", ($"value" % 20).cast("int"))

    UiPrinter.printBatchResult("groups after validation", cleaned.groupBy("bucket").agg(sum("value")).count())
  }
}

object ConfigValidationCase extends LabCase {
  override val id: String = "14_config_validation"
  override val description: String = "The Environment tab confirms which Spark properties are actually active."
  override val uiFocus: Seq[String] = Seq("Environment")

  override def runBaseline(spark: SparkSession): Unit = {
    printConfigEvidence(spark)
    UiPrinter.printBatchResult("simple groups", simpleJob(spark))
  }

  override def runOptimized(spark: SparkSession): Unit = {
    printConfigEvidence(spark)
    UiPrinter.printBatchResult("simple groups", simpleJob(spark))
  }

  private def simpleJob(spark: SparkSession): Long = {
    import spark.implicits._
    spark.range(0, 100000, 1, 16)
      .withColumn("bucket", ($"id" % 16).cast("int"))
      .groupBy("bucket")
      .count()
      .count()
  }

  private def printConfigEvidence(spark: SparkSession): Unit = {
    val conf = spark.sparkContext.getConf
    val keys = Seq(
      "spark.master",
      "spark.app.name",
      "spark.eventLog.enabled",
      "spark.eventLog.dir",
      "spark.sql.adaptive.enabled",
      "spark.sql.shuffle.partitions",
      "spark.serializer",
      "spark.driver.host",
      "spark.executor.memory"
    )

    println("Configuration evidence to compare with the Spark UI Environment tab:")
    keys.foreach { key =>
      val value = conf.getOption(key)
        .orElse(Try(spark.conf.get(key)).toOption)
        .getOrElse("<not set>")
      UiPrinter.printBatchResult(key, value)
    }
  }
}
