import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale

object PagecountsRDD {
  // Gets Language's name from its code
  def getLangName(code: String): String = {
    new Locale(code).getDisplayLanguage(Locale.ENGLISH)
  }

  def main(args: Array[String]) {
    val inputFile = args(0) // Get input file's name from this command line argument
    val conf = new SparkConf().setAppName("PagecountsRDD")
    val sc = new SparkContext(conf)

    val t0 = System.currentTimeMillis
    val pageCounts = sc.textFile(inputFile)

    val outRDD = pageCounts
      // Split each line on the spaces to get an Array with the values
      .map(row => row.split(' '))
      // We transform the RDD[Row] into a RDD[PageViewData], for compile time type safety
      .map(csv => PageViewData(csv(0).split('.')(0), csv(1), csv(2).toLong))
      // Filter lines with Page title = Language code
      .filter(pvd => pvd.Language_code != pvd.PageTitle)
      // Create (K,V) pairs where the key is the language code (the part before the first dot)
      // and the value is a tuple (Page title, View count, View count)
      .map(pvd =>
        (pvd.Language_code,
         (pvd.PageTitle, pvd.PageViewCount, pvd.PageViewCount))
       )
      // The first element of the value tuple will be the title of the page with the most views,
      // the second is the viewcount of that page and the third is the total number of views
      // for the current language.
      // When reducing, we check which of the two pairs has the higher viewcount, and keep
      // that page and viewcount as the "best" one and we add up the viewcount to the total views accumulator
      .reduceByKey((x, y) =>
                     if (x._2 > y._2) (x._1, x._2, x._3 + y._3)
                     else (y._1, y._2, x._3 + y._3),
                   1)
      // We sort the results by the total number of views, descending
      .sortBy(_._2._3, false)
      // We format the string
      .map((r) =>
        s"${getLangName(r._1)},${r._1},${r._2._3},${r._2._1},${r._2._2}")

    // Write output
    val bw = new BufferedWriter(new OutputStreamWriter(System.out, "UTF-8"))
    bw.write(
      "Language,Language-code,TotalViewsInThatLang,MostVisitedPageInThatLang,ViewsOfThatPage\n")
    outRDD.collect.foreach(x => bw.write(x + "\n"))
    bw.close

    val et = (System.currentTimeMillis - t0) / 1000
    System.err.println(
      "Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
  }
}
