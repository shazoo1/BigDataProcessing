import org.apache.avro.generic.GenericData
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField}

object MachineLearning {
  def main(args: Array[String]) : Unit = {

    val csvFile = "data/OnlineRetail.csv"

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

    val dataSetSQL = new DataSetSQL(sparkSession, csvFile)
    val dataFrameOfInvoicesAndStockCodes = dataSetSQL.getInvoiceNoAndStockCode()

    val keyValue = dataFrameOfInvoicesAndStockCodes.rdd.map(row => (row(0), row(1).toString))
    val groupedKeyValue = keyValue.groupByKey()
    val transactions = groupedKeyValue.map(row => row._2.toArray.distinct)

    //get frequent patterns via FPGrowth
    val fpg = new FPGrowth()
      .setMinSupport(0.02)

    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    //get association rules
    val minConfidence = 0.01
    val rules2 = model.generateAssociationRules(minConfidence)
    val rules = rules2.sortBy(r => r.confidence, ascending = false)

    val dataFrameOfStockCodeAndDescription = dataSetSQL.getStockCodeAndDescription()
    val dictionary = dataFrameOfStockCodeAndDescription.rdd.map(row => (row(0).toString, row(1).toString)).collect().toMap

    rules.collect().foreach { rule =>
      println(
        rule.antecedent.map(s => dictionary(s)).mkString("[", ",", "]")
          + " => " + rule.consequent.map(s => dictionary(s)).mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }
}
