name := "TweetHashtagCount"

version := "1.0"

organization := "edu.ateneo.nrg"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.1"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
"org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
"org.apache.bahir" %% "spark-streaming-twitter" % "2.3.2" 
)
