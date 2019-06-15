package edu.ateneo.nrg.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.SparkFiles._ 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.tuning._




/** Run the Logistic regression algorithm on banking data. */
object BankingClassification {
  

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    val log = Logger.getLogger("edu.ateneo.nrg.spark.BankingClassification")
    log.setLevel(Level.INFO)
        
    // Create a SparkContext using every core of the local machine
    val spark = SparkSession
        .builder
        .appName("BankingClassification")
        .master("local[*]")
        .getOrCreate()
  
    // Load each line of the source data into an RDD
    val customerData = spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", ",")
        .option("quote", "\"")
        .option("inferSchema", "true")
        .load(SparkFiles.get("banking.csv"))
    
    customerData.printSchema()
    
    val jobIndexer = new StringIndexer()
        .setInputCol("job")
        .setOutputCol("jobIndex")

    val maritalIndexer = new StringIndexer()
        .setInputCol("marital")
        .setOutputCol("maritalIndex")

    val educationIndexer = new StringIndexer()
        .setInputCol("education")
        .setOutputCol("educationIndex")

    val defaultIndexer = new StringIndexer()
        .setInputCol("default")
        .setOutputCol("defaultIndex")

    val housingIndexer = new StringIndexer()
        .setInputCol("housing")
        .setOutputCol("housingIndex")

    val loanIndexer = new StringIndexer()
        .setInputCol("loan")
        .setOutputCol("loanIndex")

    val contactIndexer = new StringIndexer()
        .setInputCol("contact")
        .setOutputCol("contactIndex")

    val monthIndexer = new StringIndexer()
        .setInputCol("month")
        .setOutputCol("monthIndex")

    val dayOfTheWeekIndexer = new StringIndexer()
        .setInputCol("day_of_week")
        .setOutputCol("dayOfTheWeekIndex")

    val pOutcomeIndexer = new StringIndexer()
        .setInputCol("poutcome")
        .setOutputCol("pOutcomeIndex")
    
    val featuresBuilder = new Pipeline()
        .setStages(Array(jobIndexer, maritalIndexer, educationIndexer, defaultIndexer, housingIndexer, loanIndexer, contactIndexer, monthIndexer, dayOfTheWeekIndexer, pOutcomeIndexer))

    val featuresBuilderModel = featuresBuilder.fit(customerData)
    
    val transformedData = featuresBuilderModel.transform(customerData)

    transformedData.printSchema()

    val splits = transformedData.randomSplit(Array(0.6, 0.4), seed = 11L)

    val trainingData = splits(0).cache()

    val testingData = splits(1)

    val encoder = new OneHotEncoderEstimator()
        .setInputCols(Array("jobIndex", "maritalIndex", "educationIndex", "pOutcomeIndex"))
        .setOutputCols(Array("jobVec", "maritalVec", "educationVec", "pOutcomeVec"))

    val assembler = new VectorAssembler()
        .setInputCols(Array("jobVec", "maritalVec", "educationVec", "defaultIndex", "housingIndex", "loanIndex", "contactIndex", "monthIndex", "dayOfTheWeekIndex", "pOutcomeVec"))
        .setOutputCol("features")

    val lr = new LogisticRegression()
        .setMaxIter(20)
        .setLabelCol("y")

    val regressionPipeline = new Pipeline()
        .setStages(Array(encoder, assembler, lr))

    // Before cross validation
    val regressionPipelineModel = regressionPipeline.fit(trainingData)
    
    val prediction = regressionPipelineModel.transform(testingData)

    val binEval = new BinaryClassificationEvaluator()
        .setRawPredictionCol("rawPrediction")
        .setLabelCol("y")

    println("Accuracy before Cross Validation", binEval.evaluate(prediction))

    val paramGrid = new ParamGridBuilder()
        .addGrid(lr.regParam, Array(0.01, 0.25, 0.5, 1.0, 2.0))
        .addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
        .addGrid(lr.maxIter, Array(1, 5, 10, 15, 20))
        .build()

    // val trainingPipelineModel = trainingPipeline.fit(transformedData)

    // val trainedData = trainingPipelineModel.transform(transformedData)

    // trainedData.select("educationVec").show()



    spark.stop()
  }
    
}
  
