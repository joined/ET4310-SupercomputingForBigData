name := "Twitterstats"

scalaVersion := "2.11.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.5.2"
libraryDependencies += "org.apache.tika" % "tika-core" % "1.13"
libraryDependencies += "org.apache.tika" % "tika-parsers" % "1.13"

resourceDirectory in Compile := baseDirectory.value / "resources"

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

