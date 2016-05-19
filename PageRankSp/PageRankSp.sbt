name := "Page Rank Spark"
version := "1.0"
scalaVersion := "2.10.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2"
libraryDependencies ++= Seq(
  "org.apache.spark"  % "spark-core_2.10"              % "1.1.0" % "provided",
    "org.apache.spark"  % "spark-mllib_2.10"             % "1.1.0"
      )
