$SPARK_HOME/bin/spark-submit \
  --class "com.corey.algorithms.SparkPageRank" \
  --master spark://Coreys-MBP:7077 \
  target/scala-2.10/simple-project_2.10-1.0.jar
