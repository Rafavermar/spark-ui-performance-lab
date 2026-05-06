package lab

import lab.cases._
import lab.data.SyntheticData
import lab.utils.{JobGroups, UiPrinter}
import org.apache.spark.sql.SparkSession

object Main {
  private val allCases: Seq[LabCase] = Seq(
    TooManyActions,
    Recomputation,
    ShuffleExplosion,
    BroadcastJoinCase,
    DataSkewCase,
    SmallFilesCase,
    TooFewPartitionsCase,
    TooManyPartitionsCase,
    SpillCase,
    CacheMisuseCase,
    UdfCostCase,
    AqeComparisonCase,
    TaskFailureRetryCase,
    ConfigValidationCase,
    StructuredStreamingBacklogCase,
    StatefulStreamingCase,
    RealTimeModeCase
  )

  private val casesById: Map[String, LabCase] = allCases.map(c => c.id -> c).toMap

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      printUsage()
      sys.exit(1)
    }

    val caseId = args(0)
    val mode = args(1)

    if (caseId == "generate_data") {
      val spark = SparkSession.builder().appName("spark-ui-lab | generate_data").getOrCreate()
      try SyntheticData.writeAll(spark)
      finally spark.stop()
      return
    }

    val labCase = casesById.getOrElse(caseId, {
      println(s"Unknown case id: $caseId")
      printUsage()
      sys.exit(1)
    })

    val spark = SparkSession.builder()
      .appName(s"spark-ui-lab | ${labCase.id} | $mode")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    UiPrinter.printCaseHeader(labCase.id, mode, labCase.description, labCase.uiFocus)

    try {
      JobGroups.withJobGroup(spark, labCase.id, mode, labCase.description) {
        labCase.run(spark, mode)
      }
      UiPrinter.pauseForInspection()
      labCase.afterInspection(spark, mode)
    } finally {
      spark.stop()
    }
  }

  private def printUsage(): Unit = {
    println("Usage: lab.Main <case_id> <mode>")
    println("Modes: baseline | optimized. Case 17 also accepts advanced.")
    println("Example: lab.Main 05_data_skew baseline")
    println("Available cases:")
    allCases.foreach(c => println(s"  ${c.id} - ${c.description}"))
  }
}
