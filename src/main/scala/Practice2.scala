import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

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
    val userMentions = sparkSession.sql("select object.twitter_entities.user_mentions from tweets")
    val mentions = userMentions.select(explode($"user_mentions").as("userMentions")).toDF()
    mentions.createOrReplaceTempView("userMentions")
    val mentionedNamesWithCounts = sparkSession.sql("select userMentions.name as userName, count(*) as " +
      "occurrences from userMentions where userMentions.Name is not null group by (userMentions.name) order by " +
      "occurrences desc")
    mentionedNamesWithCounts.show(10)

    //Display top 10 hashtags with occurrences
    val hashtags = sparkSession.sql("select twitter_entities.hashtags as hashtags from tweets")
    var hashtagsStruct = hashtags.select(explode($"hashtags")).toDF()
    hashtagsStruct.printSchema()
    hashtagsStruct.createOrReplaceTempView("hashtags")
    val hashtagsElements = sparkSession.sql("select col.text as tag, count(*) as occurrences from " +
      "hashtags where col.text is not null group by (tag) order by occurrences ")
    hashtagsElements.show(20)


    sparkSession.sql("select body from tweets").show(10)
    sparkSession.sql("select actor.languages, count(*) as occurrences from tweets where actor.languages " +
    "is not null group by actor.languages order by occurrences desc").show(10)

    //Display all tweets of selected user. User input required
    //val userName = StdIn.readLine()
    //sparkSession.sql(s"select * from tweets where (actor.displayName = \'$userName\')").show(5)

    //Display users with their tweets amount
    sparkSession.sql("select first(actor.id) as id, first(actor.displayName) as userName, count(*) as tweets " +
    "from tweets where actor.displayName is not null group by (actor.id) order by tweets desc").show()

    //Working with csv
    val csvPath = "data/gdelt.csv"
    val csvFile = sparkSession.read
      .option("header", "false")
      .option("delimiter", "\t")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(csvPath)
    csvFile.show(10)
  }
}
