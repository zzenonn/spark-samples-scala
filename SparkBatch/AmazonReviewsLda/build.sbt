name := "AmazonReviewsLda"

version := "2.0"

organization := "edu.ateneo.nrg"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
)
