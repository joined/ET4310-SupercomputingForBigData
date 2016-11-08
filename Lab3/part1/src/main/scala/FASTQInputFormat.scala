import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream}
import org.apache.hadoop.util.LineReader
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

class FASTQInputFormat extends FileInputFormat[NullWritable, Text] {
  /**
   * The input format is used only to specify the custom record reader
   */
  def createRecordReader(genericSplit: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, Text] = {
    context.setStatus(genericSplit.toString)
    new FASTQRecordReader(context.getConfiguration)
  }

  /**
   * Custom record reader implementation
   */
  class FASTQRecordReader(conf: Configuration) extends RecordReader[NullWritable, Text] {

    private var key: NullWritable = _
    private var value: Text = _
    private var start: Long = _
    private var end: Long = _
    private var pos: Long = _
    private var fileIn: FSDataInputStream = _
    private var lineReader: LineReader = _

    private val buffer: Text = new Text()

    /**
     * Initialization of the record reader
     */
    def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
      val split = genericSplit.asInstanceOf[FileSplit]
      val job = context.getConfiguration

      start = split.getStart
      end = start + split.getLength
      val file = split.getPath

      val fs = file.getFileSystem(job)
      fileIn = fs.open(file)

      // Position to the first valid read
      positionAtFirstValidRead()

      // Once we're positioned, we return the line reader
      lineReader = new LineReader(fileIn)
    }

    /**
     * Used to position at the first valid read
     */
    def positionAtFirstValidRead() {
      if (start > 0) {
        fileIn.seek(start)

        // Use a temporary line reader
        var reader = new LineReader(fileIn)
        var bytesRead = 0
        // Used to signal if we have found a valid starting read
        var readFound = false

        do {
          bytesRead = reader.readLine(buffer, (end - start).toInt)

          // If we've not read the (possible) first line of the read,
          // keep reading lines updating the start position
          if (bytesRead > 0 && !(buffer.getLength() > 0 & buffer.getBytes()(0) == '@')) {
            start += bytesRead
          } else {
            // If we've read the (possible) first line of the read,
            // which means a line starting with a '@' symbol,
            // store the position of the end of the line to start again from
            // there if the current read is not valid
            val backtrackPosition = start + bytesRead

            // Read two additional lines to check if it's a valid read
            for (_ <- 1 to 2) {
              bytesRead = reader.readLine(buffer, (end - start).toInt)
            }

            // If the third line of the read starts with a plus sign, we're good to go
            if (bytesRead > 0 && buffer.getLength() > 0 && buffer.getBytes()(0) == '+') {
              readFound = true
            } else {
              // Otherwise, we start again from the end of the line which we thought was
              // the start of a good read
              start = backtrackPosition
              fileIn.seek(start)
              reader = new LineReader(fileIn)
            }
          }
        } while (bytesRead > 0 && !readFound)

        // Once we've found a good read we position the file stream
        fileIn.seek(start)
      }
      // If start == 0 we assume it's a valid record
      pos = start
    }

    /**
     * Emit new (key, value) pair
     */
    def nextKeyValue(): Boolean = {
      synchronized {
        // Check that we didn't reach yet the end of the split
        if (pos < end) {
          val lines = new ListBuffer[String]()
          val tempLine: Text = new Text()

          // Read the lines composing the record, add them to the list
          for (i <- 1 to 4) {
            pos += lineReader.readLine(tempLine, 1000000)
            lines += tempLine.toString
          }

          // Set the value concatenating the lines
          value = new Text(lines.mkString("\n"))
          true
        } else {
          false
        }
      }
    }

    /**
     * Retrieve current key
     */
    def getCurrentKey(): NullWritable = key

    /**
     * Retrieve current value
     */
    def getCurrentValue(): Text = value

    /**
     * Retrieve progress
     */
    def getProgress(): Float = {
      if (start == end) 0.0f
      else Math.min(1.0f, (pos - start).toFloat / (end - start).toFloat)
    }

    /**
     * Close reading
     */
    def close() {
      synchronized {
        fileIn.close()
      }
    }
  }
}
