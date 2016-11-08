import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.SparkSession
import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListener}
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.io.{Text, ArrayWritable}

import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar

object Bacon {
  // This is how Kevin Bacon's name appears in the input file for male actors
  val KevinBacon = "Bacon, Kevin (I)"
  // Flag to choose if RDDs must be compressed. Has effect only if RDDs are stored serialized
  val compressRDDs = false
  // Storage level to use for RDD persistance
  val storageLevel = StorageLevel.MEMORY_ONLY

  // SparkListener must log its output in file sparkLog.txt
  val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("sparkLog.txt"), "UTF-8"))

  // Used to get the current timestamp as [HH:mm:ss]
  def getTimeStamp(): String = {
    return s"[${new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())}]"
  }

  def main(args: Array[String]) {
    val cores = args(0) // Number of cores to use
    val inputFileM = args(1) // Path of input file for male actors
    val inputFileF = args(2) // Path of input file for female actors (actresses)

    // In Spark 2.0, SparkSession is the new entry point
    val spark = SparkSession.builder
      .master(s"local[$cores]")
      .appName("Kevin Bacon app")
      // We need to increase the max result size because it can exceed the default (1g)
      .config("spark.driver.maxResultSize", "2g")
      .config("spark.rdd.compress", compressRDDs.toString)
      // Use Kryo serialization for improved performance
      // .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate

    val sc = spark.sparkContext

    // Set log level to ERROR 
    sc.setLogLevel("ERROR")

    // Use SparkListener to profile the memory consumption of cached RDDs
    sc.addSparkListener(new SparkListener() {
      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
        stageCompleted
          .stageInfo
          .rddInfos
          .foreach(rdd => {
            // If the RDD is cached show its size in memory and disk, and the number of partitions
            if (rdd.isCached) {
              bw.write(s"${getTimeStamp()} RDD ${rdd.name}: memsize = ${(rdd.memSize / (1024*1024))}MB, diskSize " +
                       s"${rdd.diskSize}, numPartitions = ${rdd.numPartitions}\n");
            } else {
            // If the RDD is not cached just say that it has been processed
              bw.write(s"${getTimeStamp()} RDD ${rdd.name}: processed!\n");
            }
        })
      }
    })

    println("Number of cores: " + args(0))
    println("Input files: " + inputFileM + " and " + inputFileF)

    var t0 = System.currentTimeMillis

    // RDD representing the male actors
    val maleActors = sc
      // Use the custom IMDBActorsInputFormat to read the IMDB actors file
      .newAPIHadoopFile[Text, ArrayWritable, IMDBActorsInputFormat](inputFileM)
      // Use a flag to indicate whether the actor is male of female.
      // true = male, false = female
      .map{case (actorName, movieList) =>
        ((actorName.toString(), true), movieList.toStrings())}
      // Cache the results since the RDD gets first "unioned" with the femaleActors RDD
      // and then it's also used in the end for the counts
      .persist(storageLevel)
      .setName("maleActors")

    // RDD representing the female actors
    val femaleActors = sc
      .newAPIHadoopFile[Text, ArrayWritable, IMDBActorsInputFormat](inputFileF)
      .map{case (actorName, movieList) =>
        ((actorName.toString(), false), movieList.toStrings())}
      // Same reasons as above
      .persist(storageLevel)
      .setName("femaleActors")

    // Create RDD with both male and female actors
    val actors = maleActors
      .union(femaleActors)
      // zipWithIndex is used to add a unique incremental ID to
      // each element of the RDD
      .zipWithIndex
      .map{case (((actorName, isMale), movieList), index) =>
        (index, (actorName, isMale, movieList))}
      // Cache the RDD since it gets branched out multiple times
      .persist(storageLevel)
      .setName("actors")

    // Get the ID of Kevin Bacon
    val baconId = actors
      .filter{case (index, (actorName, isMale, movieList)) =>
        isMale && actorName == KevinBacon}
      .setName("baconId")
      .first
      ._1

    // Create RDD of (movie, actor) pairs
    val actorsMovies = actors
      .flatMap{case (index, (actorName, isMale, movieList)) =>
        movieList.map(movie =>
          (movie, (index, actorName, isMale)))}
      // Cache the RDD since it gets branched out multiple times
      .persist(storageLevel)
      .setName("actorsMovies")

    // Get a list of (actor1, actor2) where the two actors have collaborated
    // in a movie. For each time actor1 and actor2 played a role in the same
    // movie we will have the tuples (actor1, actor2) and (actor2, actor1)
    // which is exactly what we want since in GraphX the graphs are oriented
    // but we are not interested in the direction when running the shortest
    // paths algorithm.
    // The will be also many duplicates but the cost of removing them
    // with distinct() is higher than the performance gained by doing so.
    val collaboratingActors = actorsMovies
      .join(actorsMovies)
      // Filter out pairs with same actor
      .filter{case (movie, (actor1, actor2)) =>
        !(actor1._2 == actor2._2 && actor1._3 == actor2._3)}
      .map{case (movie, (actor1, actor2)) =>
        (actor1, actor2)}
      .setName("collaboratingActors")

    // Create the edges of the graph
    val edgeTuples: RDD[(VertexId, VertexId)] = collaboratingActors
      .map{case ((index1, actorName1, isMale1), (index2, actorName2, isMale2)) =>
        (index1.toLong, index2.toLong)}
      .setName("edgeTuples")

    // Create graph in which the actors represent the vertices
    // and there is an edge between them if they have collaborated in a movie
    val graph = Graph.fromEdgeTuples(edgeTuples, false)

    // Run shortest paths algorithm using the Kevin Bacon vertex
    // as target
    val shortestPaths = ShortestPaths.run(graph, Seq(baconId))

    // The result is a graph in which each vertex has an attribute
    // consisting of the distance to Kevin Bacon's vertex
    val actorDistance = shortestPaths
      // Get the vertices of the graph
      .vertices
      // Filter the ones that are not linked to Kevin Bacon (distance = infinity)
      .filter{case (actorId, shortestPathMap) => !shortestPathMap.isEmpty}
      // Join the result back with the actors/actresses RDD to get their names
      .join(actors)
      // Remap to keep only the information we are interested in
      .map{case (actorId, (shortestPathMap, (actorName, isMale, movieList))) =>
        (actorName, isMale, shortestPathMap.head._2)}
      // Cache since the RDD gets branched multiple times
      .persist(storageLevel)
      .setName("actorsDistance")

    // Print the output
    val actorsCount = actors.count
    val maleActorsCount = maleActors.count
    val femaleActorsCount = femaleActors.count

    // Get number of unique movies reducing by the movie title so each movie
    // gets counted only one time
    val moviesNumber = actorsMovies
      .reduceByKey{case (a, b) => a}
      .setName("moviesNumber")
      .count

    println(s"Total number of actors = ${actorsCount}, out of which ${maleActorsCount} " +
            f"(${maleActorsCount.toFloat/actorsCount * 100}%1.2f%%) are males " +
            f"while ${femaleActorsCount} (${femaleActorsCount.toFloat/actorsCount * 100}%1.2f%%) are females.")

    println(s"Total number of movies = ${moviesNumber}\n")

    for (distance <- 1 to 6) {
      val actorsAtCurrentDistance = actorDistance
        .filter{case (actorName, isMale, dist) => dist == distance}
        // Cache the RDD since it gets branched out
        .persist(storageLevel)
        .setName(s"actorsAtCurrentDistance ($distance)")
      val femaleActorsAtCurrentDistanceCount = actorsAtCurrentDistance
        .filter{case (actorName, isMale, dist) => !isMale}
        .setName(s"femaleActorsAtCurrentDistance ($distance)")
        .count
      val maleActorsAtCurrentDistanceCount = actorsAtCurrentDistance
        .filter{case (actorName, isMale, dist) => isMale}
        .setName(s"maleActorsAtCurrentDistance ($distance)")
        .count

      println(s"There are ${maleActorsAtCurrentDistanceCount} " +
              f"(${maleActorsAtCurrentDistanceCount.toFloat/maleActorsCount * 100}%1.2f%%) " +
              s"and ${femaleActorsAtCurrentDistanceCount} " +
              f"(${femaleActorsAtCurrentDistanceCount.toFloat/femaleActorsCount * 100}%1.2f%%) " +
              s"actresses at distance ${distance}")
    }

    val actorsFrom1to6 = actorDistance
      .filter{case (actorName, isMale, distance) =>  1 to 6 contains distance}
      // Cache the RDD since it gets branched out
      .persist(storageLevel)
      .setName("actorsFrom1to6")
    val maleActorsFrom1to6 = actorsFrom1to6
      .filter{case (actorName, isMale, distance) => isMale}
      .setName("maleActorsFrom1to6")
    val femaleActorsFrom1to6 = actorsFrom1to6
      .filter{case (actorName, isMale, distance) => !isMale}
      .setName("femaleActorsFrom1to6")

    val actorsFrom1to6count = actorsFrom1to6.count
    val maleActorsFrom1to6count = maleActorsFrom1to6.count
    val femaleActorsFrom1to6count = femaleActorsFrom1to6.count

    println(s"\nTotal number of actors from distance 1 to 6 = ${actorsFrom1to6count}, " +
            f"ratio = ${actorsFrom1to6count.toFloat/actorsCount}%1.7f")
    println(s"Total number of male actors from distance 1 to 6 = ${maleActorsFrom1to6count}, " +
            f"ratio = ${maleActorsFrom1to6count.toFloat/maleActorsCount}%1.7f")
    println(s"Total number of female actors from distance 1 to 6 = ${femaleActorsFrom1to6count}, " +
            f"ratio = ${femaleActorsFrom1to6count.toFloat/femaleActorsCount}%1.7f")

    val actorsAtDistance6 = actorsFrom1to6
      .filter{case (actorName, isMale, distance) => distance == 6}
      // Cache since the RDD gets branched out in two
      .persist(storageLevel)
      .setName("actorsAtDistance6")
    val maleActorsAtDistance6 = actorsAtDistance6
      .filter{case (actorName, isMale, distance) => isMale}
      .setName("maleActorsAtDistance6")
    val femaleActorsAtDistance6 = actorsAtDistance6
      .filter{case (actorName, isMale, distance) => !isMale}
      .setName("femaleActorsAtDistance6")

    println("\nList of male actors at distance 6:")

    maleActorsAtDistance6
      .sortBy{case (actorName, isMale, distance) => actorName}
      .zipWithIndex
      .collect
      .foreach{case ((actorName, isMale, distance), index) =>
        println(s"${index+1}. ${actorName}")}

    println("\nList of female actors at distance 6:")
    femaleActorsAtDistance6
      .sortBy{case (actorName, isMale, distance) => actorName}
      .zipWithIndex
      .collect
      .foreach{case ((actorName, isMale, distance), index) =>
        println(s"${index+1}. ${actorName}")}

    sc.stop()
    bw.close()

    val et = (System.currentTimeMillis - t0) / 1000
    println("{Time taken = %d mins %d secs}".format(et / 60, et % 60))
  }
}
