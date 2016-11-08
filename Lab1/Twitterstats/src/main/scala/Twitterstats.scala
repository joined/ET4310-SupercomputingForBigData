import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern

object Twitterstats {
  var firstTime = true
  var t0: Long = 0
  val bw = new BufferedWriter(
    new OutputStreamWriter(new FileOutputStream("twitterLog.txt"), "UTF-8"))

  // This function will be called periodically after each 5 seconds to log the output.
  def write2Log(data: Array[TwitterStatsData]) {
    if (firstTime) {
      bw.write(
        "Seconds,Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,MaxRetweetCount,MinRetweetCount,RetweetCount,Text\n")
      t0 = System.currentTimeMillis
      firstTime = false
    } else {
      val seconds = (System.currentTimeMillis - t0) / 1000

      if (seconds < 60) {
        println(
          s"Elapsed time = $seconds seconds. Logging will be started after 60 seconds.")
        return
      }

      println(s"""Logging the output to the log file
                |Elapsed time = $seconds seconds
                |-----------------------------------""".stripMargin)

      for (i <- 0 until data.size) {
        val langCode = data(i).langCode
        val lang = getLangName(langCode)
        val totalRetweetsInThatLang = data(i).totalRetweetsInThatLang
        val id = data(i).idOfOriginalTweet
        val textStr = data(i).text.replaceAll("\\r|\\n", " ")
        val minRetweetCount = data(i).minRetweetCount
        val maxRetweetCount = data(i).maxRetweetCount
        val retweetCount = data(i).retweetCount

        bw.write(
          s"($seconds),$lang,$langCode,$totalRetweetsInThatLang,$id,$maxRetweetCount,$minRetweetCount,$retweetCount,$textStr\n")
      }
    }
  }

  // Pass the text of the retweet to this function to get the Language (in two letter code form) of that text.
  def getLang(s: String): String = {
    val inputStr = s
      .replaceFirst("RT", "")
      .replaceAll("@\\p{L}+", "")
      .replaceAll("https?://\\S+\\s?", "")

    var langCode = new LanguageIdentifier(inputStr).getLanguage

    // Detect if japanese
    var pat = Pattern.compile("\\p{InHiragana}")
    var m = pat.matcher(inputStr)
    if (langCode == "lt" && m.find) { langCode = "ja" }

    // Detect if korean
    pat = Pattern.compile("\\p{IsHangul}");
    m = pat.matcher(inputStr)
    if (langCode == "lt" && m.find) { langCode = "ko" }

    return langCode
  }

  // Gets Language's name from its code
  def getLangName(code: String): String = {
    return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
  }

  def main(args: Array[String]) {
    // Configure Twitter credentials
    val apiKey = "..."
    val apiSecret = "..."
    val accessToken = "..."
    val accessTokenSecret = "..."

    Helper.configureTwitterCredentials(apiKey,
                                       apiSecret,
                                       accessToken,
                                       accessTokenSecret)

    val ssc = new StreamingContext(new SparkConf(), Seconds(5))
    val tweets = TwitterUtils.createStream(ssc, None)

    // Add your code here
    val retweets = tweets
      .filter(status => status.isRetweet)
      .window(Seconds(60))
      .map(status => {
        val text = status.getText
        val retweetedStatus = status.getRetweetedStatus
        (text,
         getLang(text),
         retweetedStatus.getId,
         retweetedStatus.getRetweetCount)
      })

    // Reduction by IDofOriginalTweet to get RetweetCount, MinRetweetCount, MaxRetweetCount
    val IDofOriginalTweet_RetweetCount = retweets
      // Create (K, V) pairs for reduce, where K = IDofOriginalTweet
      // and V = (originalTweetRetweetCount, originalTweetRetweetCount).
      // The first element of the value tuple is used as minRetweetCount and
      // the second is used as maxRetweetCount
      .map(t => (t._3, (t._4, t._4)))
      // We keep the minimum retweetCount as minRetweetCount and the
      // maximum retweetCount as maxRetweetCount.
      .reduceByKey((x, y) => (math.min(x._1, y._1), math.max(x._2, y._2)))
      // The result of the reduction is
      // (IDofOriginalTweet, (minRetweetCount, maxRetweetCount)).
      // We map the results such that the resulting (K, V) pair is
      // (IDofOriginalTweet, (minRetweetCount, maxRetweetCount, retweetCount))
      .map(t => (t._1, (t._2._1, t._2._2, t._2._2 - t._2._1 + 1)))

    // Join retweets and IDofOriginalTweet_RetweetCount to add counts info to each row
    val retweetsWithCounts = retweets
      // Remap so we have (IDofOriginalTweet, (text, langCode))
      .map(t => (t._3, (t._1, t._2)))
      // After the join we'll have
      // (IDofOriginalTweet, ((text, langCode), (minRetweetCount, maxRetweetCount, retweetCount)))
      .join(IDofOriginalTweet_RetweetCount)
      // Flatten keeping IDofOriginalTweet as key, so we have
      // (IDofOriginalTweet, (text, langCode, minRetweetCount, maxRetweetCount, retweetCount))
      .map(t =>
        (t._1, (t._2._1._1, t._2._1._2, t._2._2._1, t._2._2._2, t._2._2._3)))

    // Reduction by the language code to get TotalRetweetsInThatLang
    val LangCode_TotalRetweetsInThatLang = retweetsWithCounts
      // We create (K, V) pairs for reduce,
      // where K = langCode and V = retweetCount
      .map(t => (t._2._2, t._2._5))
      // Afer the reduce we have (langCode, TotalRetweetsInThatLang)
      .reduceByKey(_ + _)

    // Joining retweetsWithCounts with LangCode_TotalRetweetsInThatLang using langCode
    // to get TotalRetweetsInThatLang for every tweet
    val outDStream = retweetsWithCounts
      // Create (K, V) pairs for join,
      // where K = langCode and V = (IDofOriginalTweet, text, minRetweetCount, maxRetweetCount, retweetCount)
      .map(t => (t._2._2, (t._1, t._2._1, t._2._3, t._2._4, t._2._5)))
      // After the join we'll have
      // (langCode, ((IDofOriginalTweet, text, minRetweetCount, maxRetweetCount, retweetCount), TotalRetweetsInThatLang))
      .join(LangCode_TotalRetweetsInThatLang)
      // Remap so that we have
      // (langCode, TotalRetweetsInThatLang, IDofOriginalTweet, text, minRetweetCount, maxRetweetCount, retweetCount)
      .map(t => (t._1, t._2._2, t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5))
      // Transform since we can't sort on DStream
      .transform(rdd =>
        // Sort by the tuple (TotalRetweetsInThatLang, retweetCount) in descending order
        rdd.sortBy(line => (line._2, line._7), ascending=false)
      )
      // Convert the structure into result type
      .map(t => TwitterStatsData(t._1, t._3, t._4, t._5, t._6, t._7, t._2))

    // Write output to log
    outDStream.foreachRDD(rdd => write2Log(rdd.collect))

    new java.io.File("cpdir").mkdirs
    ssc.checkpoint("cpdir")
    ssc.start()
    ssc.awaitTermination()
  }
}

case class TwitterStatsData(langCode: String,
                            idOfOriginalTweet: Long,
                            text: String,
                            minRetweetCount: Long,
                            maxRetweetCount: Long,
                            retweetCount: Long,
                            totalRetweetsInThatLang: Long)
