import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }

import java.io._

/**
 * Collection of methods to interact with the Hadoop Distributed File System
 */
object HDFSHelper {
  val conf = new Configuration()
  val hdfs = FileSystem.get(conf)

  /**
   * Used to copy a file from the HDFS to the local file system
   */
  def copyToLocalFile(hdfsPath: String, localPath: String) = {
    val hdfsRes = new Path(hdfsPath)
    val localRes = new Path(localPath)

    hdfs.copyToLocalFile(hdfsRes, localRes)
  }

  /**
   * Used to copy a file from the local file system to HDFS
   */
  def copyFromLocalFile(localPath: String, hdfsPath: String) = {
    val hdfsRes = new Path(hdfsPath)
    val localRes = new Path(localPath)

    hdfs.copyFromLocalFile(localRes, hdfsRes)
  }

  /**
   * Create directories in HDFS
   */
  def mkdirs(hdfsPath: String) = {
    val hdfsRes = new Path(hdfsPath)

    hdfs.mkdirs(hdfsRes)
  }

  /**
   * Delete a file if it exists and create a zero length file in its place
   */
  def createEmptyFile(hdfsPath: String) = {
    val hdfsRes = new Path(hdfsPath)

    // If the file already exists, delete it
    if (hdfs.exists(hdfsRes)) { hdfs.delete(hdfsRes) }

    // Create a zero length file
    hdfs.createNewFile(hdfsRes)
  }

  /**
   * Append a line to an existing file
   */
  def appendLineToFile(hdfsPath: String, line: String) = {
    val hdfsRes = new Path(hdfsPath)

    val outputStream = hdfs.append(hdfsRes)
    val br = new BufferedWriter(new OutputStreamWriter(outputStream))

    br.write(s"$line\n")
    br.close
  }

  /**
   * List the files in a HDFS folder
   */
  def listFiles(hdfsPath: String) = {
    val hdfsRes = new Path(hdfsPath)

    val status = hdfs.listStatus(hdfsRes)

    status.filter(_.isFile).map(_.getPath.getName).toList
  }
}
