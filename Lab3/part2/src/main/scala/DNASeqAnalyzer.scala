/*
 * Copyright (c) 2015-2016 TU Delft, The Netherlands.
 * All rights reserved.
 *
 * You can redistribute this file and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This file is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Authors: Hamid Mushtaq
 * Modified by: Lorenzo Gasparini
 *
*/
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.sys.process._
import scala.io.Source

import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar

import tudelft.utils.{ ChromosomeRange, Configuration, SAMRecordIterator }

import htsjdk.samtools._

object DNASeqAnalyzer {
  val MemString = "-Xmx5120m"
  val RefFileName = "ucsc.hg19.fasta"
  val SnpFileName = "dbsnp_138.hg19.vcf"
  val ExomeFileName = "gcat_set_025.bed"


  /**
   * Used to get the current timestamp as [HH:mm:ss]
   */
  def getTimeStamp(): String = {
    return s"[${new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())}]"
  }

  /**
   * Run Burrow-Wheeler Aligner for pairwise alignment
   */
  def bwaRun(chunkFile: String, config: Configuration): Array[(Int, SAMRecord)] = {
    val refFolder = config.getRefFolder
    val tmpFolder = config.getTmpFolder
    val numOfThreads = config.getNumThreads
    val toolsFolder = config.getToolsFolder
    val inputFolder = config.getInputFolder
    val outputFolder = config.getOutputFolder
    val bwaLogFolder = s"$outputFolder/log/bwa"
    val bwaLogFileName = s"$bwaLogFolder/$chunkFile.log"

    // File that will contain the temporary output from BWA
    val tmpOutputFileName = s"${chunkFile.dropRight(4)}.tmp.sam"
    val tmpOutputFile = new File(s"$tmpFolder/$tmpOutputFileName")

    // Create the BWA log folder if it doesn't already exist
    new File(bwaLogFolder).mkdirs

    val cmd = Seq(s"$toolsFolder/bwa", "mem", s"$refFolder/$RefFileName", "-p",
      "-t", numOfThreads, s"$inputFolder/$chunkFile")

    // Create output log writer with auto flush enabled
    val bwaLogWriter = new PrintWriter(new BufferedWriter(new FileWriter(bwaLogFileName)), true)
    bwaLogWriter.println(s"$getTimeStamp BWA mem started: ${cmd.mkString(" ")}")

    // Run the BWA program and redirect STDOUT to the temporary file
    (cmd #> tmpOutputFile).!

    val bwaKeyValues = new BWAKeyValues(s"$tmpFolder/$tmpOutputFileName")
    bwaKeyValues.parseSam()
    val kvPairs: Array[(Int, SAMRecord)] = bwaKeyValues.getKeyValuePairs()

    bwaLogWriter.println(s"$getTimeStamp BWA mem completed. Number of key value pairs: ${kvPairs.length}")

    // Delete the temporary file
    tmpOutputFile.delete
    // Close the log file
    bwaLogWriter.close

    // Return the key-value pairs
    kvPairs
  }

  /**
   * Write the SAM records to the BAM file
   */
  def writeToBAM(fileName: String, samRecordsSorted: Array[SAMRecord], config: Configuration): ChromosomeRange = {
    val header = new SAMFileHeader()
    header.setSequenceDictionary(config.getDict())

    val outHeader = header.clone()
    outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate)

    val factory = new SAMFileWriterFactory()
    val writer = factory.makeBAMWriter(outHeader, true, new File(fileName))

    val chrRange = new ChromosomeRange()
    val input = new SAMRecordIterator(samRecordsSorted, header, chrRange)

    while (input.hasNext) {
      writer.addAlignment(input.next)
    }

    writer.close

    // Return the chromosome range
    chrRange
  }

  /**
   * Perform variant calling on a list of SAM records for a given region
   */
  def variantCall(chrRegion: Int, samRecordsSorted: Array[SAMRecord], config: Configuration): Array[(Int, (Int, String))] = {
    val tmpFolder = config.getTmpFolder
    val toolsFolder = config.getToolsFolder
    val refFolder = config.getRefFolder
    val numOfThreads = config.getNumThreads
    val outputFolder = config.getOutputFolder
    val vcLogFolder = s"$outputFolder/log/vc"
    val vcLogFileName = s"$vcLogFolder/region$chrRegion.log"

    // Create the VC log folder if it doesn't already exist
    new File(vcLogFolder).mkdirs

    // Create output log writer
    val vcLogWriter = new PrintWriter(new BufferedWriter(new FileWriter(vcLogFileName)), true)
    vcLogWriter.println(s"$getTimeStamp Writing SAM records to BAM file")

    // SAM records are sorted by this point
    val chrRange = writeToBAM(s"$tmpFolder/region$chrRegion-p1.bam", samRecordsSorted, config)

    // Picard preprocessing
    val cmd1 = Seq("java", MemString, "-jar", s"$toolsFolder/CleanSam.jar",
      s"INPUT=$tmpFolder/region$chrRegion-p1.bam",
      s"OUTPUT=$tmpFolder/region$chrRegion-p2.bam")
    vcLogWriter.println(s"$getTimeStamp ${cmd1.mkString(" ")}")
    cmd1.!

    val cmd2 = Seq("java", MemString, "-jar", s"$toolsFolder/MarkDuplicates.jar",
      s"INPUT=$tmpFolder/region$chrRegion-p2.bam",
      s"OUTPUT=$tmpFolder/region$chrRegion-p3.bam",
      s"METRICS_FILE=$tmpFolder/region$chrRegion-p3-metrics.txt")
    vcLogWriter.println(s"$getTimeStamp ${cmd2.mkString(" ")}")
    cmd2.!

    val cmd3 = Seq("java", MemString, "-jar", s"$toolsFolder/AddOrReplaceReadGroups.jar",
      s"INPUT=$tmpFolder/region$chrRegion-p3.bam",
      s"OUTPUT=$tmpFolder/region$chrRegion.bam",
      "RGID=GROUP1", "RGLB=LIB1", "RGPL=ILLUMINA", "RGPU=UNIT1", "RGSM=SAMPLE1")
    vcLogWriter.println(s"$getTimeStamp ${cmd3.mkString(" ")}")
    cmd3.!

    val cmd4 = Seq("java", MemString, "-jar", s"$toolsFolder/BuildBamIndex.jar",
      s"INPUT=$tmpFolder/region$chrRegion.bam")
    vcLogWriter.println(s"$getTimeStamp ${cmd4.mkString(" ")}")
    cmd4.!

    // Delete temporary files
    new File(s"$tmpFolder/region$chrRegion-p1.bam").delete
    new File(s"$tmpFolder/region$chrRegion-p2.bam").delete
    new File(s"$tmpFolder/region$chrRegion-p3.bam").delete
    new File(s"$tmpFolder/region$chrRegion-p3-metrics.txt").delete

    // Make region file
    val tmpBedFileName = s"$tmpFolder/tmp$chrRegion.bed"
    val tmpBedFile = new File(tmpBedFileName)
    chrRange.writeToBedRegionFile(tmpBedFile.getAbsolutePath())

    val cmd5 = Seq(s"$toolsFolder/bedtools", "intersect", "-a",
      s"$refFolder/$ExomeFileName", "-b",
      s"$tmpFolder/tmp$chrRegion.bed", "-header")
    vcLogWriter.println(s"$getTimeStamp ${cmd5.mkString(" ")}")

    val outputBedFileName = s"$tmpFolder/bed$chrRegion.bed"
    val outputBedFile = new File(outputBedFileName)

    (cmd5 #> outputBedFile).!

    // Delete temporary file
    tmpBedFile.delete

    // Indel Realignment
    val cmd6 = Seq("java", MemString, "-jar", s"$toolsFolder/GenomeAnalysisTK.jar",
      "-T", "RealignerTargetCreator", "-nt", numOfThreads, "-R",
      s"$refFolder/$RefFileName", "-I", s"$tmpFolder/region$chrRegion.bam",
      "-o", s"$tmpFolder/region$chrRegion.intervals", "-L",
      s"$tmpFolder/bed$chrRegion.bed")
    vcLogWriter.println(s"$getTimeStamp ${cmd6.mkString(" ")}")
    cmd6.!

    val cmd7 = Seq("java", MemString, "-jar", s"$toolsFolder/GenomeAnalysisTK.jar",
      "-T", "IndelRealigner", "-R", s"$refFolder/$RefFileName",
      "-I", s"$tmpFolder/region$chrRegion.bam", "-targetIntervals",
      s"$tmpFolder/region$chrRegion.intervals", "-o",
      s"$tmpFolder/region$chrRegion-2.bam", "-L", s"$tmpFolder/bed$chrRegion.bed")
    vcLogWriter.println(s"$getTimeStamp ${cmd7.mkString(" ")}")
    cmd7.!

    // Delete temporary files
    new File(s"$tmpFolder/region$chrRegion.bam").delete
    new File(s"$tmpFolder/region$chrRegion.bai").delete
    new File(s"$tmpFolder/region$chrRegion.intervals").delete

    // Base quality recalibration
    val cmd8 = Seq("java", MemString, "-jar", s"$toolsFolder/GenomeAnalysisTK.jar", "-T",
      "BaseRecalibrator", "-nct", numOfThreads, "-R", s"$refFolder/$RefFileName",
      "-I", s"$tmpFolder/region$chrRegion-2.bam", "-o",
      s"$tmpFolder/region$chrRegion.table", "-L", s"$tmpFolder/bed$chrRegion.bed",
      "--disable_auto_index_creation_and_locking_when_reading_rods",
      "-knownSites", s"$refFolder/$SnpFileName")
    vcLogWriter.println(s"$getTimeStamp ${cmd8.mkString(" ")}")
    cmd8.!

    val cmd9 = Seq("java", MemString, "-jar", s"$toolsFolder/GenomeAnalysisTK.jar",
      "-T", "PrintReads", "-R", s"$refFolder/$RefFileName", "-I",
      s"$tmpFolder/region$chrRegion-2.bam", "-o",
      s"$tmpFolder/region$chrRegion-3.bam", "-BQSR",
      s"$tmpFolder/region$chrRegion.table", "-L", s"$tmpFolder/bed$chrRegion.bed")
    vcLogWriter.println(s"$getTimeStamp ${cmd9.mkString(" ")}")
    cmd9.!

    // Delete temporary files
    new File(s"$tmpFolder/region$chrRegion-2.bam").delete
    new File(s"$tmpFolder/region$chrRegion-2.bai").delete
    new File(s"$tmpFolder/region$chrRegion.table").delete

    // Haplotype -> Uses the region bed file
    val cmd10 = Seq("java", MemString, "-jar", s"$toolsFolder/GenomeAnalysisTK.jar", "-T",
      "HaplotypeCaller", "-nct", numOfThreads, "-R", s"$refFolder/$RefFileName",
      "-I", s"$tmpFolder/region$chrRegion-3.bam", "-o",
      s"$tmpFolder/region$chrRegion.vcf", "-stand_call_conf", "30.0",
      "-stand_emit_conf", "30.0", "-L", s"$tmpFolder/bed$chrRegion.bed",
      "--no_cmdline_in_header",
      "--disable_auto_index_creation_and_locking_when_reading_rods")
    vcLogWriter.println(s"$getTimeStamp ${cmd10.mkString(" ")}")
    cmd10.!
    vcLogWriter.println(s"$getTimeStamp Output written to VCF file")

    vcLogWriter.close

    // Delete remaining temporary files
    outputBedFile.delete
    new File(s"$tmpFolder/region$chrRegion-3.bam").delete
    new File(s"$tmpFolder/region$chrRegion-3.bai").delete
    new File(s"$tmpFolder/region$chrRegion.vcf.idx").delete

    // Read the ouput VCF file produced by the haplotype caller
    val source = Source.fromFile(s"$tmpFolder/region$chrRegion.vcf").getLines

    // Return the content of the file in the format
    // (Chromosome number, (Chromosome Position, Line))
    source
      // Valid lines start with "chr", we want to skip all the others
      .filter(_.startsWith("chr"))
      .map { line =>
        val splittedLine = line.split("\t")
        val rawChromosomeNumber = splittedLine(0).drop(3)
        val position = splittedLine(1).toInt

        val chrNum = rawChromosomeNumber match {
          case "X" => 23
          case "Y" => 24
          case _ => rawChromosomeNumber.toInt
        }

        (chrNum, (position, line))
      }
      .toArray
  }

  def main(args: Array[String]) {
    val config = new Configuration()
    config.initialize()

    // Get config values from the configuration XML
    val inputFolder = config.getInputFolder
    val outputFolder = config.getOutputFolder
    val tmpFolder = config.getTmpFolder
    val numInstances = config.getNumInstances

    // In Spark 2.0, SparkSession is the new entry point
    val spark = SparkSession.builder
      .appName("DNASeqAnalyzer")
      .master(s"local[$numInstances]")
      .config("spark.cores.max", numInstances)
      .getOrCreate

    val sc = spark.sparkContext

    // Set log level to ERROR
    sc.setLogLevel("ERROR")

    var t0 = System.currentTimeMillis

    // Create temporary files, output and log folders if they don't exist
    new File(tmpFolder).mkdirs
    new File(outputFolder).mkdirs
    new File(s"$outputFolder/log/").mkdirs

    // Get the list of files in the input folder
    val chunkList = new File(inputFolder)
      .listFiles
      .filter(_.isFile)
      .map(_.getName)
      .toList

    // Create the RDD with <chrNumber, SAMrecord> elements
    val chrToSamRecord = sc
      .parallelize(chunkList)
      .flatMap(bwaRun(_, config))
      // Cache it because we'll use it multiple times
      .cache

    // Compute number of SAM records for each chromosome number
    // and sort it by descending number of SAM records
    val chrToNumSamRecords = chrToSamRecord
      .map { case (chr, samRecord) => (chr, 1) }
      .reduceByKey(_ + _)
      .sortBy({ case (chr, numSamRecords) => numSamRecords }, false)
      .collect

    // In this step we do load balancing. We want to assign to each chromosome number,
    // which goes from 1 to 24, a group number which goes
    // from 0 to numInstances-1 (e.g., from 0 to 3 for 4 instances) and we want the groups
    // to have more or less the same number of SAM records.
    // To do this we first create an array the same length as the number of groups in which
    // we store the number of SAM records assigned to each group,
    // and a Map in which we will store the chrNumber -> groupNumber association.
    val recordsPerGroup = Array.fill[Long](numInstances.toInt)(0)
    val loadBalancingMap = scala.collection.mutable.Map[Int, Int]()

    // Then we iterate over all the chromosome numbers, which are ordered by descending
    // number of SAM records. Each iteration we assign the current chromosome number
    // to the group that has the least amount of SAM records assigned.
    // Then we update the SAM records count of each group accordingly.
    chrToNumSamRecords.foreach {
      case (chr, numSamRecords) =>
        val leastRecordsGroupIndex = recordsPerGroup.indexOf(recordsPerGroup.min)
        recordsPerGroup(leastRecordsGroupIndex) += numSamRecords
        loadBalancingMap += (chr -> leastRecordsGroupIndex)
    }
    // This solution gives a SAM record count for each group of
    // [277662, 279898, 273353, 278221] which looks close enough.

    // Compute the balanced RDD by converting the chromosome number to the chromosome group,
    // then repartition it so that each element of the RDD goes in the partition corresponding
    // to its chromosome group
    val chrToSamRecsBalanced = chrToSamRecord
      .map { case (chr, samRecord) => (loadBalancingMap(chr), samRecord) }
      .partitionBy(new HashPartitioner(numInstances.toInt))

    // Operate the variant call on each partition separately
    val variantCallOutput = chrToSamRecsBalanced
      .mapPartitionsWithIndex {
        case (chrRegion, iterator) =>
          // The chromosome region will be the same as the partition ID
          // because of the way HashPartitioner works (hashes are hash(chrNumber) % numInstances)

          // Create an Array with all the SAM records of the current group sorted
          val samRecordsSorted = iterator
            .map { case (chr, samRecord) => samRecord }
            .toArray
            .sortWith {
              case (samRec1, samRec2) =>
                // Get chromosome numbers
                val (chrNum1, chrNum2) = (samRec1.getReferenceIndex, samRec2.getReferenceIndex)
                // If the chromosome numbers are equal, the ordering is decided by the starting position
                if (chrNum1 == chrNum2) samRec1.getAlignmentStart < samRec2.getAlignmentStart
                // Otherwise the order is decided by the chromosome number
                else chrNum1 < chrNum2
            }

          // Return the iterator over the results array
          variantCall(chrRegion, samRecordsSorted, config).toIterator
      }
      // We want to be sure that the variant call step will be executed only once,
      // so we cache the RDD
      .cache

    // Combine the results, sort them and write them to output file
    val outputVcfWriter = new PrintWriter(new BufferedWriter(new FileWriter(s"${outputFolder}result.vcf")), true)

    variantCallOutput
      .sortBy { case (chrNum, (position, line)) => (chrNum, position) }
      .map { case (chrNum, (position, line)) => line }
      .collect
      .foreach(line => outputVcfWriter.println(line))

    outputVcfWriter.close

    val et = (System.currentTimeMillis - t0) / 1000
    println("|Execution time: %d mins %d secs|".format(et / 60, et % 60))

    sc.stop
  }
}
