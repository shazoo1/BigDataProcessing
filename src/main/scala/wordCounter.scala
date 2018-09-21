import java.lang.Exception
import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

import scala.Exception


object wordCounter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("Practice 1")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val file = sc.textFile("data/products.csv")

    println("Started to map .csv file")
    val termsRDD = file.flatMap(x => x.split(" "))
    val terms = termsRDD.map(x => (x, 1))

    val reducedTerms = terms.reduceByKey((accum, i) => accum + i)
    reducedTerms.collect

    //Try to delete file to prevent exception on writing
    try {
      val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("127.0.0.1"),
        sc.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path("data/output/wordsCount"), true) // isRecusrive= true
    }
    catch {
      case e: Exception => println(e);
    }
    reducedTerms.saveAsTextFile("data/output/wordsCount")
  }
}