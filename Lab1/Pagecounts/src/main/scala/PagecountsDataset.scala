import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io._
import java.util.Locale

object PagecountsDataset {
  // Gets Language's name from its code
  def getLangName(code: String): String = {
    new Locale(code).getDisplayLanguage(Locale.ENGLISH)
  }

  def main(args: Array[String]) {
    val inputFile = args(0) // Get input file's name from this command line argument
    // In Spark 2.0, SparkSession is the new entry point
    val spark = SparkSession.builder.appName("PagecountsDataset").getOrCreate

    // We import encoders for the most common types and implicit conversions
    import spark.implicits._

    val t0 = System.currentTimeMillis

    // We read the input file
    val lines = spark.read.text(inputFile)

    // We transform the Dataset[Row] into a Dataset[PageViewData], for compile time type safety
    val pageCounts: Dataset[PageViewData] = lines
      .map(row => row.getString(0).split(' '))
      // First element of the row is splitted to get the first part before the first dot
      .map(csv => PageViewData(csv(0).split('.')(0), csv(1), csv(2).toLong))
      // We filter out records for which the language code is the same of the page title
      .filter('Language_code =!= 'PageTitle)

    // We declare a new UDF to get the language name from the code
    val getLangNameUDF = udf({ langCode: String =>
      getLangName(langCode)
    })

    // We use a window to calculate for each row the most visited page in the row's language
    // and the view count of that page. The window is ordered by descending view count
    val byLanguageCodeWindow =
      Window.partitionBy('Language_code).orderBy('PageViewCount.desc)

    val outDataset = pageCounts
      // Add column with most visited page in the language, which is the first in the window
      .withColumn("MostVisitedPageInThatLang",
                  lead('PageTitle, 0) over byLanguageCodeWindow)
      // Add column with number of views of the most visited page, which is the first of the window
      .withColumn("ViewsOfThatPage",
                  lead('PageViewCount, 0) over byLanguageCodeWindow)
      .groupBy('Language_code)
      // The most visited page is found in every row of the corresponding language, so we
      // just take the first. The same holds for the views of that page
      .agg(sum('PageViewCount) as "TotalViewsInThatLang",
           first('MostVisitedPageInThatLang) as "MostVisitedPageInThatLang",
           first('ViewsOfThatPage) as "ViewsOfThatPage")
      // We sort by the number of total views of the language
      .sort($"TotalViewsInThatLang".desc)
      // And add the name of the language using the UDF
      .withColumn("Language", getLangNameUDF('Language_code))

    // Write output
    val bw = new BufferedWriter(new OutputStreamWriter(System.out, "UTF-8"))
    bw.write(
      "Language,Language-code,TotalViewsInThatLang,MostVisitedPageInThatLang,ViewsOfThatPage\n")
    outDataset.collect.foreach(pvd =>
      bw.write(s"${pvd(4)},${pvd(0)},${pvd(1)},${pvd(2)},${pvd(3)}\n"))
    bw.close

    val et = (System.currentTimeMillis - t0) / 1000
    System.err.println(
      "Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
  }
}

// Case class representing the source data
case class PageViewData(Language_code: String,
                        PageTitle: String,
                        PageViewCount: Long)
