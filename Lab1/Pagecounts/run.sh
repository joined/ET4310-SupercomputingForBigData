spark-submit --class "$1" --driver-memory 4g --master "local[4]" target/scala-2.11/Pagecounts-assembly-0.1-SNAPSHOT.jar $2
