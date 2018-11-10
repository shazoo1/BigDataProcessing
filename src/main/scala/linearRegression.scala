import java.util.logging.{Level, Logger}

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object linearRegression {
  val sparkSession : SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .getOrCreate()
  val csvFilePath = "./data/diabets.csv"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("rg").setLevel(Level.OFF)

    val diabetsDf = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(csvFilePath)

    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.3)
      .setElasticNetParam(0.5)

    val DFAssembler = new VectorAssembler()
      .setInputCols(Array(
      "pregnancy", "glucose", "arterial pressure",
      "thickness of TC", "insulin", "body mass index",
      "heredity", "age"))
      .setOutputCol("features")

    val features = DFAssembler.transform(diabetsDf)
    features.show(10)

    val labeledTransformer = new StringIndexer()
      .setInputCol("diabet")
      .setOutputCol("label")
    val labeledFeatures = labeledTransformer
      .fit(features)
      .transform(features)

    val splits = labeledFeatures.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)

    val trainingModel = lr.fit(trainingData)

    println(s"Coefficients: ${trainingModel.coefficients} Intercept: ${trainingModel.intercept}")

    //Make predictions on test data
    val predictions = trainingModel.transform(testData)

    //Evaluate the precision and recall
    val countProve = predictions.where("label == prediction").count()
    val count = predictions.count()

    println(s"Count of true predictions: $countProve Total Count: $count")
  }
}
