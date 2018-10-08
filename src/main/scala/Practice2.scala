import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

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

    import sparkSession.implicits._

    //Show top 10 mentioned people with mentions counts
    val twitterEntities = sparkSession.sql("select object.twitter_entities.user_mentions from tweets")
    val mentions = twitterEntities.select(explode($"user_mentions").as("userMentions")).toDF()
    mentions.createOrReplaceTempView("userMentions")
    val mentionedNamesWithCounts = sparkSession.sql("select userMentions.name as userName, count(*) as " +
      "occurrences from userMentions where userMentions.Name is not null group by (userMentions.name) order by " +
      "occurrences desc")
    mentionedNamesWithCounts.show(10)

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
    sparkSession.sql("select actor.languages, count(*) as occurrences from tweets where actor.languages " +
    "is not null group by actor.languages order by occurrences desc").show(10)

    //Display all tweets of selected user. User input required
    val userName = StdIn.readLine()
    sparkSession.sql(s"select * from tweets where (actor.displayName = \'$userName\')").show(5)

    //Display users with their tweets amount
    sparkSession.sql("select first(actor.id) as id, first(actor.displayName) as userName, count(*) as tweets " +
    "from tweets where actor.displayName is not null group by (actor.id) order by tweets desc").show()

  }
}
