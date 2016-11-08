import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io._

object AnalyzeTwitters {
  def main(args: Array[String]) {
    val inputFile = args(0) // Get input file's name from this command line argument
    // In Spark 2.0, SparkSession is the new entry point
    val spark = SparkSession.builder.appName("PagecountsDataset").getOrCreate

    // We import encoders for the most common types and implicit conversions
    import spark.implicits._

    val t0 = System.currentTimeMillis

    // We read the file as a csv, skipping columns we don't need
    val inputDataset = spark.read
      .option("header", "true")
      .csv(inputFile)
      .map((r) => {
        TwitterAnalysisData(
                            // Skip the seconds, we don't need them
                            r.getString(1),
                            // Skip TotalRetweetsInThatLang, we don't need it
                            r.getString(2),
                            r.getString(4).toLong,
                            r.getString(5).toLong,
                            r.getString(6).toLong,
                            r.getString(7).toLong,
                            r.getString(8))
      })

    // We create a Dataset in which each Tweet ID is associated with the RetweetCounts
    val ID_RetweetCounts = inputDataset
      .groupBy('IDOfTweet)
      .agg(max('MaxRetweetCount) as "MaxRetweetCounts",
           min('MinRetweetCount) as "MinRetweetCounts")
      // Add column containing computed RetweetCounts
      .withColumn("RetweetCounts", 'MaxRetweetCounts - 'MinRetweetCounts + 1)
      // Drop useless columns
      .drop("MaxRetweetCounts", "MinRetweetCounts")

    // We use a window to calculate for each tweet the TotalRetweetsInThatLang
    val byLanguageCodeWindow = Window.partitionBy('Language_code)

    val outDataset = inputDataset
      // To get the RetweetCounts for each IDOfTweet we join the datasetts
      .join(ID_RetweetCounts, "IDOfTweet")
      // Drop useless columns
      .drop("RetweetCount", "MaxRetweetCount", "MinRetweetCount")
      // Remove duplicate tweets
      .distinct
      // Add column with RetweetCounts
      .withColumn("TotalRetweetsInThatLang",
                  sum('RetweetCounts) over byLanguageCodeWindow)
      // Sort by descending total retweet counts in the lang and
      // descending retweet counts of the tweet
      .sort($"TotalRetweetsInThatLang".desc, $"RetweetCounts".desc)
      // Keep only columns with more than 1 retweet
      .filter('RetweetCounts > 1)

    // Write output
    val bw = new BufferedWriter(new OutputStreamWriter(System.out, "UTF-8"))
    bw.write(
      "Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,RetweetCount,Text\n")
    outDataset.collect.foreach(
      tad =>
        bw.write(
          s"${tad(1)},${tad(2)},${tad(5)},${tad(0)},${tad(4)},${tad(3)}\n"))
    bw.close

    val et = (System.currentTimeMillis - t0) / 1000
    System.err.println(
      "Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
  }
}

// Case class representing the source data
case class TwitterAnalysisData(Language: String,
                               Language_code: String,
                               IDOfTweet: Long,
                               MaxRetweetCount: Long,
                               MinRetweetCount: Long,
                               RetweetCount: Long,
                               Text: String)
