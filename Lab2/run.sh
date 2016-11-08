#!/bin/sh
# Modified by Lorenzo Gasparini so that we pass n. of CPU cores here and RAM is variable

display_usage() {
  echo "\nUsage:\n$0 [cpu_cores] [memory] [actors_file] [actresses_file]\n"
}

if [ $# -le 3 ]; then
  display_usage
  exit 1
fi

echo "Driver memory: $2"
spark-submit --class "Bacon" --driver-memory $2 ./target/scala-2.11/bacon_2.11-1.0.jar $1 $3 $4
