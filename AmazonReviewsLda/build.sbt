name := "AmazonReviewsLda"

version := "1.0"

organization := "edu.ateneo.nrg"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
)
