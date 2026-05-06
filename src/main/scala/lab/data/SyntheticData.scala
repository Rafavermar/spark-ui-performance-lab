package lab.data

import lab.utils.{FsUtils, LabPaths}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SyntheticData {
  def fact(spark: SparkSession): DataFrame =
    if (FsUtils.exists(spark, LabPaths.generated("fact"))) spark.read.parquet(LabPaths.generated("fact"))
    else factFrame(spark, 180000)

  def dim(spark: SparkSession): DataFrame =
    if (FsUtils.exists(spark, LabPaths.generated("dim_customers"))) spark.read.parquet(LabPaths.generated("dim_customers"))
    else dimFrame(spark)

  def skew(spark: SparkSession): DataFrame =
    if (FsUtils.exists(spark, LabPaths.generated("skew_events"))) spark.read.parquet(LabPaths.generated("skew_events"))
    else skewFrame(spark, 180000)

  def factFrame(spark: SparkSession, rows: Long): DataFrame = {
    import spark.implicits._
    spark.range(0, rows, 1, 24)
      .withColumn("customer_id", ($"id" % 1000).cast("long"))
      .withColumn("country_id", ($"id" % 25).cast("int"))
      .withColumn("category_id", ($"id" % 80).cast("int"))
      .withColumn("event_day", date_add(to_date(lit("2026-01-01")), ($"id" % 30).cast("int")))
      .withColumn("amount", (($"id" % 1000).cast("double") / 10.0) + 1.0)
      .withColumn("is_active", ($"id" % 5) =!= 0)
      .withColumn("payload_a", sha2(concat_ws("-", lit("a"), $"id".cast("string")), 256))
      .withColumn("payload_b", sha2(concat_ws("-", lit("b"), $"id".cast("string")), 256))
      .withColumn("payload_c", sha2(concat_ws("-", lit("c"), $"id".cast("string")), 256))
  }

  def dimFrame(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.range(0, 1000, 1, 1)
      .withColumnRenamed("id", "customer_id")
      .withColumn("segment", concat(lit("segment_"), ($"customer_id" % 10).cast("string")))
      .withColumn("region", concat(lit("region_"), ($"customer_id" % 5).cast("string")))
  }

  def skewFrame(spark: SparkSession, rows: Long): DataFrame = {
    import spark.implicits._
    val hotUntil = (rows * 0.78).toLong
    spark.range(0, rows, 1, 32)
      .withColumn("join_key", when($"id" < hotUntil, lit(0L)).otherwise(($"id" % 8000).cast("long")))
      .withColumn("salt_seed", ($"id" % 16).cast("int"))
      .withColumn("metric", ($"id" % 100).cast("double"))
  }

  def writeAll(spark: SparkSession): Unit = {
    println("Generating deterministic synthetic datasets under data/generated/ ...")
    Seq("fact", "dim_customers", "skew_events", "small_files").foreach(name => FsUtils.delete(spark, LabPaths.generated(name)))

    factFrame(spark, 180000).repartition(24).write.mode("overwrite").parquet(LabPaths.generated("fact"))
    dimFrame(spark).coalesce(1).write.mode("overwrite").parquet(LabPaths.generated("dim_customers"))
    skewFrame(spark, 180000).repartition(32).write.mode("overwrite").parquet(LabPaths.generated("skew_events"))

    import spark.implicits._
    spark.range(0, 2400, 1, 80)
      .withColumn("bucket", ($"id" % 40).cast("int"))
      .withColumn("payload", concat(lit("record-"), $"id".cast("string")))
      .repartition(80)
      .write.mode("overwrite").json(LabPaths.generated("small_files"))

    println("Synthetic datasets generated.")
  }
}
