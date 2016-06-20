name := "SparkonJobserver"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Job server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.6.2" % "provided"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0"
    