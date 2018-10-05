import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

object superheroes {
  def main(args: Array[String]): Unit = {
  val conf = new SparkConf()

  conf.setAppName("Practice superheroes")
  conf.setMaster("local[2]")

  val sc = new SparkContext(conf)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val file = sc.textFile("data/Superheroes.txt")
  val rows = file.map(_.split(";"))
  val superheroes = rows.map(x => (x(1),x(2)))

  val superheroesWithOnes = superheroes.map(x => (x._1,1))
  val superheroesCountReduced = superheroesWithOnes.reduceByKey(_ + _)
    superheroesCountReduced.collect()
  val superHeroesStringified = superheroesCountReduced.map(x => x.toString().mkString(" "))
    superHeroesStringified.map(x => x.mkString("")).foreach(println)

    val superHeroesKillsTuples = superheroes.reduceByKey((x,y) => (x.toInt + y.toInt).toString)
    superHeroesKillsTuples.collect()
    superHeroesKillsTuples.map(x => (x._1, ": ",x._2)).foreach(println)
  //superheroesCountReduced.map(x => ((x(0),": ",x(1)))).foreach(println)
}
}
