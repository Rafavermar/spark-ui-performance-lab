package lab.cases

import org.apache.spark.sql.SparkSession

trait LabCase {
  def id: String
  def description: String
  def uiFocus: Seq[String]

  def runBaseline(spark: SparkSession): Unit
  def runOptimized(spark: SparkSession): Unit

  def run(spark: SparkSession, mode: String): Unit =
    mode match {
      case "baseline" => runBaseline(spark)
      case "optimized" => runOptimized(spark)
      case other => throw new IllegalArgumentException(s"Unsupported mode '$other' for $id. Use baseline or optimized.")
    }

  def afterInspection(spark: SparkSession, mode: String): Unit = ()
}
