name := "DNASeqAnalyzer"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.1"
libraryDependencies += "com.github.samtools" % "htsjdk" % "1.143"
