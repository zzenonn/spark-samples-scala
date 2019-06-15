# Banking Binary Logistic Regression Classification

This next application demonstrates the use of the Binary Logistic Regression classifier on banking AML data. After some minimal changes, this can even be run on a cluster.

To compile, run `sbt assembly`. To run, run `spark-submit --files banking.csv target/scala-2.11/BankingClassification-assembly-1.0.jar`

*Note: It is highly suggested that you use a virtual machine to run this application. As this requires very heavy processing, this may crash your computer.*
