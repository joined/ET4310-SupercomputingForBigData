import org.apache.spark.sql.SparkSession
import org.apache.spark.TaskContext
import org.apache.hadoop.io.{NullWritable, Text}

import java.io._
import java.util.zip.GZIPOutputStream

import scala.util.Random

object FastqChunker {
  def main(args: Array[String]) {
    if (args.size < 3) {
      println("""Not enough arguments!
                |Arg1 = number of parallel tasks = number of chunks
                |Arg2 = input folder
                |Arg3 = output folder""".stripMargin)
      System.exit(1)
    }

    val prllTasks = args(0)
    val inputFolder = args(1)
    val outputFolder = args(2)

    // Create file resource representing the input folder
    val inputPath = new File(inputFolder)

    // Check that the input folder exists and is a folder
    if (!inputPath.isDirectory) {
      println(s"Input folder $inputFolder doesn't exist or is not a folder!")
      System.exit(1)
    }

    // Make complete identifier of the input path, so that the program
    // doesn't try to look in the HDFS if the Hadoop env. variable is set
    val completeInputPath = s"file:///${inputPath.getAbsolutePath}"

    // Create output folder if it doesn't already exist
    new File(outputFolder).mkdirs

    println(s"""Number of parallel tasks = number of chunks = $prllTasks
               |Input folder = $inputFolder
               |Output folder = $outputFolder""".stripMargin)

    // In Spark 2.0, SparkSession is the new entry point
    val spark = SparkSession.builder
      .master(s"local[$prllTasks]")
      .appName("FastqChunker")
      .config("spark.cores.max", prllTasks)
      .getOrCreate

    val sc = spark.sparkContext

    // Set log level to ERROR
    sc.setLogLevel("ERROR")

    var t0 = System.currentTimeMillis

    // Rest of the code goes here
    val inputFile1 = s"$completeInputPath/fastq1.fq"
    val inputFile2 = s"$completeInputPath/fastq2.fq"

    // Read first FASTQ input file into an RDD using the custom input format
    // The resulting PairRDD elements have as the value the FASTQ read
    // (made by 4 lines). The key is not used and is set to the null type.
    val fastq1 = sc
      .newAPIHadoopFile[NullWritable, Text, FASTQInputFormat](inputFile1)
      .map{case (_, read) =>
        read.toString}

    // Second FASTQ input file
    val fastq2 = sc
      .newAPIHadoopFile[NullWritable, Text, FASTQInputFormat](inputFile2)
      .map{case (_, read) =>
        read.toString}

    // Zip the first and the second RDD. Zip takes one element from the first
    // RDD (= one read) and one from the second RDD and combines them into a tuple
    // in the resulting RDD
    val result = fastq1
      .zip(fastq2)
      // Concatenate the two reads
      .map{case (read1, read2) => s"$read1\n$read2\n"}
      // Repartition the RDD to use the same number of partitions as the number
      // of tasks given from the command line
      .repartition(prllTasks.toInt)

    // Iterate over all the partitions
    result
      .foreachPartition{iterator =>
        // Get the ID of the partition to use in the output file name
        val partitionId = TaskContext.getPartitionId
        val outputFile = s"$outputFolder/chunk$partitionId.fq.gz"
        val outputStream = new FileOutputStream(outputFile)
        val zipOutputStream = new GZIPOutputStream(outputStream)
        while (iterator.hasNext) {
          zipOutputStream.write(iterator.next.getBytes)
        }
        zipOutputStream.close()
      }

    val et = (System.currentTimeMillis - t0) / 1000
    println("|Execution time: %d mins %d secs|".format(et / 60, et % 60))

    sc.stop
  }
}
