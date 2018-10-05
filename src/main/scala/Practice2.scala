import org.apache.spark.sql.SparkSession

import scala.io.StdIn

object Practice2 {

  def main(args: Array[String]) : Unit = {

    val jsonFile = "data/sampletweets.json"

    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-tweets")
      .master("local[*]")
      .getOrCreate()

    val tweets = sparkSession.read.json(jsonFile)

    tweets.printSchema()
    tweets.createOrReplaceTempView("tweets")

    //Display top 10 hashtags with occurrences. Not working properly
    val hashtags = sparkSession.sql("select twitter_entities.hashtags as twentities, count(*) as count " +
      "from tweets where (twitter_entities.hashtags is not null) group by " +
      "(twitter_entities.hashtags) order by count desc limit 25")
    val hashtagsElements = hashtags.rdd.map(_(0))
    /*hashtagTexts.reduceByKey(_ + _)
    hashtagTexts.collect()
    hashtagTexts.foreach(println)*/
    hashtagsElements.foreach(println)


    sparkSession.sql("select body from tweets").show(10)
    sparkSession.sql("select actor.languages, count(*) as occurences from tweets where actor.languages " +
    "is not null group by actor.languages order by occurences desc").show(10)

    //Display all tweets of selected user. User input required
    val userName = StdIn.readLine()
    sparkSession.sql(s"select * from tweets where (actor.displayName = \'$userName\')").show(5)

    //Display users with their tweets amount
    sparkSession.sql("select first(actor.id) as id, first(actor.displayName) as userName, count(*) as tweets " +
    "from tweets where actor.displayName is not null group by (actor.id) order by tweets desc").show()

  }
}
