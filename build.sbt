ThisBuild / organization := "lab"
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := sys.props.getOrElse("scala.version", sys.env.getOrElse("SCALA_VERSION", "2.13.17"))

val sparkVersion = sys.props.getOrElse("spark.version", sys.env.getOrElse("SPARK_VERSION", "4.1.1"))

lazy val root = (project in file("."))
  .settings(
    name := "spark-ui-performance-lab",
    Compile / mainClass := Some("lab.Main"),
    assembly / mainClass := Some("lab.Main"),
    assembly / assemblyJarName := "spark-ui-performance-lab-assembly-0.1.0.jar",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "org.apache.spark" %% "spark-token-provider-kafka-0-10" % sparkVersion
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case "module-info.class" => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
