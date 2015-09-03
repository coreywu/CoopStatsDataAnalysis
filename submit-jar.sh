echo $SPARK_HOME
$SPARK_HOME/bin/spark-submit \
  --class "com.corey.simpleapp.SimpleApp" \
  --master local[4] \
  target/scala-2.10/simple-project_2.10-1.0.jar
