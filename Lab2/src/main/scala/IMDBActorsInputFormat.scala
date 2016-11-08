import scala.collection.mutable.ListBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, ArrayWritable}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, LineRecordReader}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

class IMDBActorsInputFormat extends FileInputFormat[Text, ArrayWritable] {

  def createRecordReader(genericSplit: InputSplit, context: TaskAttemptContext): RecordReader[Text, ArrayWritable] = {
    context.setStatus(genericSplit.toString)
    new IMDBActorsRecordReader(context.getConfiguration)
  }

  class IMDBActorsRecordReader(conf: Configuration) extends RecordReader[Text, ArrayWritable] {

    // Use double newline as a delimiter, so that a "line" corresponds to an actor with its movies
    private val lineRecordReader = new LineRecordReader(Array('\n'.toByte, '\n'.toByte))
    private var key: Text = new Text()
    private var value: ArrayWritable = _
    private var innerValue: Text = _
    private var actorGroup: Array[Byte] = _
    private var actorGroupLength: Int = _
    private var actorGroupString: String = _
    private var splittedActorGroup: Array[String] = _

    def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
      lineRecordReader.initialize(genericSplit, context)
    }

    // Used to get the cleaned title of the movie given a line of the input file
    // Returns Some[String] if there is a valid movie title, otherwise None
    def getCleanedMovie(rawMovieString: String): Option[String] = {
      // Get the part of the line from where the title starts
      var secondPart = rawMovieString.slice(rawMovieString.lastIndexOf("\t") + 1, rawMovieString.length)

      // Skip the TV series, which are enclosed in ""
      if (secondPart.startsWith("\"")) {
        return None
      }

      // Get the index of the first character of the movie's year
      val indexOfYear = secondPart.indexOf("(") + 1
      // Get the string corresponding to the year
      val yearString = secondPart.slice(indexOfYear, indexOfYear + 4)

      // Check that the year is valid (digits) and >= 2011
      if (!yearString.isEmpty && yearString.forall(_.isDigit) && yearString.toInt >= 2011)
        Some(secondPart.slice(0, indexOfYear + 4) + ")")
      else
        None
    }

    // Used to emit a new (actor, movieList) pair
    def nextKeyValue(): Boolean = {
      synchronized {
        // The do-while loop is necessary because if the actor we are analyzing doesn't have
        // any valid movies, we don't have a (actor, movieList) pair to emit so we must look
        // at the next actor until we find a "valid" one.
        val movies = new ListBuffer[String]()

        do {
          if (lineRecordReader.nextKeyValue()) {
            innerValue = lineRecordReader.getCurrentValue
            actorGroup = innerValue.getBytes
            actorGroupLength = innerValue.getLength

            // The actor group is only valid until actorGroupLength characters.
            // The file is encoded in latin1 format (ISO-8859-1)
            actorGroupString = new String(actorGroup.slice(0, actorGroupLength), "ISO-8859-1")

            splittedActorGroup = actorGroupString.split('\n')

            // Split the first line on the tab, the actor name is the part before the tab
            key.set(splittedActorGroup(0).split('\t')(0))

            // Empty movie list
            movies.clear()

            // Add movies to list
            splittedActorGroup.foreach(line => getCleanedMovie(line).foreach(movieTitle => movies += movieTitle))
          } else {
            // End of the split
            return false
          }
        // If the movie list was empty (i.e. no movie was valid) repeat until we find a valid actor
        } while (movies.isEmpty)

        value = new ArrayWritable(movies.toArray)
        true
      }
    }

    def getCurrentKey(): Text = key
    def getCurrentValue(): ArrayWritable = value
    def getProgress(): Float = lineRecordReader.getProgress

    def close() {
      synchronized {
        lineRecordReader.close()
      }
    }
  }
}

