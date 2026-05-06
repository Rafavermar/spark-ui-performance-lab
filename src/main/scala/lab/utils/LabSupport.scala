package lab.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

object LabPaths {
  val Workspace = "/workspace"
  val Generated = s"$Workspace/data/generated"
  val Tmp = s"$Workspace/tmp"

  def generated(name: String): String = s"$Generated/$name"
  def tmp(name: String): String = s"$Tmp/$name"
  def checkpoint(name: String): String = s"/opt/spark-checkpoints/$name"
}

object FsUtils {
  def exists(spark: SparkSession, path: String): Boolean = {
    val fs = new Path(path).getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.exists(new Path(path))
  }

  def delete(spark: SparkSession, path: String): Unit = {
    val fs = new Path(path).getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(path), true)
  }
}

object UiPrinter {
  def printCaseHeader(id: String, mode: String, description: String, tabs: Seq[String]): Unit = {
    println()
    println(s"=== Spark UI Performance Lab: $id [$mode] ===")
    println(description)
    println(s"Spark UI focus tabs: ${tabs.mkString(", ")}")
    println("Live Spark Application UI: http://localhost:4040")
    println("Spark Master UI:          http://localhost:8080")
    println("Spark History Server:     http://localhost:18080")
    println()
  }

  def pauseForInspection(): Unit = {
    val autoExit = sys.env.get("LAB_AUTO_EXIT").exists(_.equalsIgnoreCase("true"))
    if (autoExit) {
      println("LAB_AUTO_EXIT=true, waiting briefly for event log flush.")
      Thread.sleep(2000L)
    } else if (System.console() != null) {
      println("Inspect the live Spark UI now. Press Enter to stop the application.")
      scala.io.StdIn.readLine()
    } else {
      println("No interactive terminal detected. Keeping the application alive for 8 seconds.")
      Thread.sleep(8000L)
    }
  }

  def printBatchResult(label: String, value: Any): Unit =
    println(f"$label%-36s $value")
}

object JobGroups {
  def withJobGroup[T](spark: SparkSession, id: String, mode: String, description: String)(body: => T): T = {
    spark.sparkContext.setJobGroup(s"$id-$mode", s"$id $mode - $description", interruptOnCancel = true)
    try body
    finally spark.sparkContext.clearJobGroup()
  }
}

object StreamingSupport {
  def remember(query: StreamingQuery): StreamingQuery = {
    println(s"Streaming query started: ${query.name}, id=${query.id}")
    println("Open the Structured Streaming tab while this query is running.")
    query
  }

  def stop(query: Option[StreamingQuery]): Unit =
    query.foreach { q =>
      if (q.isActive) {
        println(s"Stopping streaming query: ${q.name}")
        q.stop()
      }
    }
}
