package lab.cases

import lab.data.SyntheticData
import lab.utils.{FsUtils, LabPaths, UiPrinter}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object TooManyActions extends LabCase {
  override val id: String = "01_too_many_actions"
  override val description: String = "Multiple unnecessary actions trigger multiple Spark jobs over the same lineage."
  override val uiFocus: Seq[String] = Seq("Jobs", "Stages")

  override def runBaseline(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = SyntheticData.factFrame(spark, 140000)
      .where($"country_id" < 20)
      .withColumn("score", length($"payload_a") + $"category_id")

    UiPrinter.printBatchResult("count action", df.count())
    UiPrinter.printBatchResult("filtered count action", df.where($"score" > 90).count())
    UiPrinter.printBatchResult("group count action", df.groupBy("category_id").count().count())
    UiPrinter.printBatchResult("sum action", df.agg(sum("amount")).collect().head.get(0))
  }

  override def runOptimized(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = SyntheticData.factFrame(spark, 140000)
      .where($"country_id" < 20)
      .withColumn("score", length($"payload_a") + $"category_id")

    val summary = df.agg(
      count(lit(1)).as("rows"),
      sum(when($"score" > 90, 1).otherwise(0)).as("high_score_rows"),
      sum("amount").as("amount_sum")
    ).collect().head

    UiPrinter.printBatchResult("rows", summary.getAs[Long]("rows"))
    UiPrinter.printBatchResult("high_score_rows", summary.getAs[Long]("high_score_rows"))
    UiPrinter.printBatchResult("amount_sum", summary.getAs[Double]("amount_sum"))
  }
}

object Recomputation extends LabCase {
  override val id: String = "02_recomputation"
  override val description: String = "The same expensive transformed DataFrame is reused without persistence."
  override val uiFocus: Seq[String] = Seq("Jobs", "Stages", "Storage")

  private var cached: Option[DataFrame] = None

  private def expensiveFrame(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.range(0, 180000, 1, 24)
      .withColumn("customer_id", ($"id" % 2000).cast("long"))
      .withColumn("bucket", ($"id" % 120).cast("int"))
      .withColumn("hash_a", sha2(concat_ws("-", $"id".cast("string"), lit("expensive-a")), 256))
      .withColumn("hash_b", sha2(concat_ws("-", $"hash_a", lit("expensive-b")), 256))
      .withColumn("score", length($"hash_a") + length($"hash_b") + $"bucket")
      .where($"score" > 100)
  }

  override def runBaseline(spark: SparkSession): Unit = {
    val df = expensiveFrame(spark)
    UiPrinter.printBatchResult("count action", df.count())
    UiPrinter.printBatchResult("distinct buckets action", df.select("bucket").distinct().count())
    UiPrinter.printBatchResult("group action", df.groupBy("bucket").agg(avg("score")).count())
  }

  override def runOptimized(spark: SparkSession): Unit = {
    val df = expensiveFrame(spark).persist(StorageLevel.MEMORY_AND_DISK)
    cached = Some(df)
    UiPrinter.printBatchResult("materialize persisted frame", df.count())
    UiPrinter.printBatchResult("distinct buckets action", df.select("bucket").distinct().count())
    UiPrinter.printBatchResult("group action", df.groupBy("bucket").agg(avg("score")).count())
    println("Storage tab should show the persisted DataFrame during inspection.")
  }

  override def afterInspection(spark: SparkSession, mode: String): Unit = {
    cached.foreach(_.unpersist(blocking = false))
    cached = None
  }
}

object ShuffleExplosion extends LabCase {
  override val id: String = "03_shuffle_explosion"
  override val description: String = "A wide groupBy/orderBy plan shuffles more data than needed."
  override val uiFocus: Seq[String] = Seq("SQL", "Stages")

  override def runBaseline(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.shuffle.partitions", "48")
    val fact = SyntheticData.fact(spark)
    val result = fact
      .groupBy("country_id", "category_id", "payload_a", "payload_b")
      .agg(sum("amount").as("amount_sum"), count(lit(1)).as("rows"))
      .orderBy(desc("amount_sum"))
      .limit(20)

    result.explain(extended = true)
    UiPrinter.printBatchResult("result rows", result.collect().length)
  }

  override def runOptimized(spark: SparkSession): Unit = {
    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.partitions", "12")
    val fact = SyntheticData.fact(spark)
    val result = fact
      .where($"is_active" && $"country_id" < 12)
      .select("country_id", "category_id", "amount")
      .groupBy("country_id", "category_id")
      .agg(sum("amount").as("amount_sum"), count(lit(1)).as("rows"))
      .orderBy(desc("amount_sum"))
      .limit(20)

    result.explain(extended = true)
    UiPrinter.printBatchResult("result rows", result.collect().length)
  }
}

object BroadcastJoinCase extends LabCase {
  override val id: String = "04_broadcast_join"
  override val description: String = "A join shuffles both sides even though one side is small."
  override val uiFocus: Seq[String] = Seq("SQL", "Stages")

  override def runBaseline(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val joined = SyntheticData.fact(spark)
      .join(SyntheticData.dim(spark), Seq("customer_id"))
      .groupBy("segment", "region")
      .agg(sum("amount").as("amount_sum"), count(lit(1)).as("rows"))

    joined.explain(extended = true)
    UiPrinter.printBatchResult("joined groups", joined.collect().length)
  }

  override def runOptimized(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20m")
    val joined = SyntheticData.fact(spark)
      .join(broadcast(SyntheticData.dim(spark)), Seq("customer_id"))
      .groupBy("segment", "region")
      .agg(sum("amount").as("amount_sum"), count(lit(1)).as("rows"))

    joined.explain(extended = true)
    UiPrinter.printBatchResult("joined groups", joined.collect().length)
  }
}

object DataSkewCase extends LabCase {
  override val id: String = "05_data_skew"
  override val description: String = "A hot key dominates a join and creates uneven task duration."
  override val uiFocus: Seq[String] = Seq("Stages", "SQL")

  private def rightSide(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.range(0, 8000, 1, 4)
      .withColumnRenamed("id", "join_key")
      .withColumn("dim_value", concat(lit("dim_"), $"join_key".cast("string")))
  }

  override def runBaseline(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "24")
    val joined = SyntheticData.skew(spark)
      .join(rightSide(spark), Seq("join_key"))
      .groupBy("join_key")
      .agg(sum("metric").as("metric_sum"))
      .orderBy(desc("metric_sum"))

    joined.explain(extended = true)
    UiPrinter.printBatchResult("top rows", joined.limit(10).collect().length)
  }

  override def runOptimized(spark: SparkSession): Unit = {
    import spark.implicits._
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "24")

    val left = SyntheticData.skew(spark)
      .withColumn("salt", when($"join_key" === 0L, $"salt_seed").otherwise(lit(0)))

    val right = rightSide(spark)
      .withColumn("salt", explode(when($"join_key" === 0L, sequence(lit(0), lit(15))).otherwise(array(lit(0)))))

    val joined = left
      .join(right, Seq("join_key", "salt"))
      .groupBy("join_key")
      .agg(sum("metric").as("metric_sum"))
      .orderBy(desc("metric_sum"))

    joined.explain(extended = true)
    UiPrinter.printBatchResult("top rows", joined.limit(10).collect().length)
  }
}

object SmallFilesCase extends LabCase {
  override val id: String = "06_small_files"
  override val description: String = "Reading many small files creates many tiny tasks and scheduling overhead."
  override val uiFocus: Seq[String] = Seq("Jobs", "Stages")

  private def ensureSmallFiles(spark: SparkSession): Unit =
    if (!FsUtils.exists(spark, LabPaths.generated("small_files"))) SyntheticData.writeAll(spark)

  override def runBaseline(spark: SparkSession): Unit = {
    ensureSmallFiles(spark)
    val raw = spark.read.json(LabPaths.generated("small_files"))
    UiPrinter.printBatchResult("input partitions", raw.rdd.getNumPartitions)
    UiPrinter.printBatchResult("rows", raw.groupBy("bucket").count().count())
  }

  override def runOptimized(spark: SparkSession): Unit = {
    ensureSmallFiles(spark)
    val compactedPath = LabPaths.tmp("case06_compacted_small_files")
    FsUtils.delete(spark, compactedPath)

    val raw = spark.read.json(LabPaths.generated("small_files"))
    raw.coalesce(8).write.mode("overwrite").parquet(compactedPath)

    val compacted = spark.read.parquet(compactedPath)
    UiPrinter.printBatchResult("input partitions", compacted.rdd.getNumPartitions)
    UiPrinter.printBatchResult("rows", compacted.groupBy("bucket").count().count())
  }
}

object TooFewPartitionsCase extends LabCase {
  override val id: String = "07_too_few_partitions"
  override val description: String = "Too little parallelism leaves executors underused."
  override val uiFocus: Seq[String] = Seq("Executors", "Stages")

  override def runBaseline(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark.range(0, 500000, 1, 1)
      .withColumn("bucket", ($"id" % 64).cast("int"))
      .withColumn("score", length(sha2($"id".cast("string"), 256)))
    UiPrinter.printBatchResult("partitions", df.rdd.getNumPartitions)
    UiPrinter.printBatchResult("groups", df.groupBy("bucket").agg(sum("score")).count())
  }

  override def runOptimized(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark.range(0, 500000, 1, 16)
      .repartition(16)
      .withColumn("bucket", ($"id" % 64).cast("int"))
      .withColumn("score", length(sha2($"id".cast("string"), 256)))
    UiPrinter.printBatchResult("partitions", df.rdd.getNumPartitions)
    UiPrinter.printBatchResult("groups", df.groupBy("bucket").agg(sum("score")).count())
  }
}
