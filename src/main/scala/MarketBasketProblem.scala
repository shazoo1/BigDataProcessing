import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MarketBasketProblem {
  def main(args: Array[String]) : Unit = {

    val csvFile = "data/OnlineRetail.csv"
    val sc = new SparkContext("local[2]", "mlLib")

    val sparkSession = SparkSession
      .builder()
      .appName("spark-machinelearning")
      .master("local[*]")
      .getOrCreate()

    val retail = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(csvFile)
    retail.show(10)

    //Reading csv as a common file
    val retailRdd = sc.textFile(csvFile)
    val transactionsFromCsv : RDD[Array[String]] = retailRdd.map(_.split(","))
    transactionsFromCsv.foreach(x => println(x.mkString("["," ","}")))

    //Getting transactions with goods names
    val invoicesWithGifts = transactionsFromCsv.map(x => (x(0),x(1)))
    val invoicesAndStockCodes = invoicesWithGifts.groupByKey()
    val stockCodesAndNames = transactionsFromCsv.map(x => (x(1),x(2).toString)).collect().toMap

    val giftsWithoutInvoices = invoicesWithGifts.map(x => x._2)
    val rddGiftsWithoutInvoices : RDD[Array[String]] = invoicesAndStockCodes.map(_._2.toArray.distinct)

    val trainingModel = new FPGrowth()
      .setMinSupport(0.02)
      .setNumPartitions(1)
      .run(rddGiftsWithoutInvoices)

    trainingModel.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    val associationRules = trainingModel.generateAssociationRules(0.01)
      .sortBy(_.confidence, false)

    associationRules.collect().foreach{ rule =>
      println(
        rule.antecedent.map(s => stockCodesAndNames(s)).mkString("[", ",", "]")
          + " => " + rule.consequent.map(s => stockCodesAndNames(s)).mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }
}
