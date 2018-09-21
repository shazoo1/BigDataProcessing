import java.lang.Exception
import java.util.logging.{Level, Logger}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.Exception


object numbers {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("Practice 1")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val file = sc.textFile("data/numbers.txt")
    val rows = file.map(_.split(" "))
    val numbers = rows.map(_.map(y => y.toInt))
    val sums = numbers.map(_.sum)

    val fiveProductionSums = numbers.map(_.filter(_ % 5 == 0)).map(_.sum)

    val maxValues = numbers.map(_.max.toString)
    val minValues = numbers.map(_.min.toString)
    val distincts = numbers.map(_.distinct)

    val flatNumbers = file.flatMap(_.split(" "))
    val flatSum = flatNumbers.reduce(_ + _)
    val flatFiveProductionsSum = flatNumbers.map(_.filter(_ % 5 == 0)).reduce(_+_)
    val flatMaxValue = flatNumbers.map(_.max.toString)
    val flatMinValue = flatNumbers.map(_.min.toString)
    val flatDistincts = flatNumbers.map(_.distinct)

    //Forming RDDs to write into a file
    val sumsRDD= sc.parallelize(Seq("Sums of each row:")) ++ sums.map(_.toString)
    val fiveProductionsRDD = sc.parallelize(Seq("Sums of all produtions of five in each row:")) ++ fiveProductionSums.map(_.toString)
    val maxsRDD = sc.parallelize(Seq("Maximums of each row:")) ++ maxValues
    val minsRDD = sc.parallelize(Seq("Minimums of each row:")) ++ minValues
    val distinctsRDD = sc.parallelize(Seq("Distinct values of each row:")) ++ distincts.map(_.toString)

    val mapRDD = sumsRDD ++ fiveProductionsRDD ++ maxsRDD ++ minsRDD ++ distinctsRDD

    val flatSumRDD = sc.parallelize(Seq("Sum of all numbers in file:\n"+flatSum))
    val flatFiveProductionsRDD = sc.parallelize(Seq("Sum of  productions in file:\n"+flatFiveProductionsSum))
    val flatMaxRDD = sc.parallelize(Seq("Maximal number in file:")) ++ flatMaxValue
    val flatMinRDD = sc.parallelize(Seq("Minimal number in file:")) ++ flatMinValue
    val flatDistinctsRDD = sc.parallelize(Seq("Distinct numbers in file:")) ++ flatDistincts

    val flatMapRDD = flatSumRDD ++ flatFiveProductionsRDD ++ flatMaxRDD ++ flatMinRDD ++ flatDistinctsRDD
    val finalRDD = mapRDD ++ flatMapRDD

    //Try to delete file to prevent crash on writing
    try {
      val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("127.0.0.1"),
        sc.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path("data/output/numbers"), true) // isRecusrive= true
    }
    catch {
      case e: Exception => println(e);
    }
    finalRDD.saveAsTextFile("data/output/numbers")
  }
}