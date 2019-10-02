
## How to run

There are multiple ways to run the spark application. 
At least the following have been verified:

- standalone local mode
- spark-submit in spark cluster

### Submit the application in IntellijIdea

In order to run in standalone mode, `.master("local[*]")` has to be added to builder.
In addition to that, the input and output arguments must be provided like this:
```
src/main/resources/train.csv target/task-1
```

### Submit the application in spark cluster
In order to run the application in spark cluster, the hdp sandbox has to be started first and 
application jar to be copied into the container.

So, the steps must be the following.

1) First, the sandbox hdp and proxy need to be started:
```
docker start sandbox-hdp
docker start sandbox-proxy
```

2) Once done, the project needs to be packaged. To do this, invoke the following command from inside the 
project folder:
```sbt clean package```

4) Then from inside the project root folder copy jar from local:
`docker cp target/scala-2.11/hotelsrecommendationsspark_2.11-0.1.jar sandbox-hdp:hotels-2.11.jar`

5) Once  done, connect to the sandbox cluster via the following command: 
```docker exec -it sandbox-hdp /bin/bash```

6) The application can be run from the root folder via the following command:

```
spark-submit \
--master=local[*] \
--class com.epam.bigdata.training.spark.hotels.Task1HotelsApplication \
--conf spark.default.parallelism=8 \
--conf spark.driver.memory=1G \
--conf spark.executor.memory=2G \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.memory.fraction=0.7 \
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=1G \
--conf spark.sql.shuffle.partitions=8 \
hotels-2.11.jar \
/user/admin/expedia_recommendations/train.csv \
/user/admin/expedia_recommendations/task-1-output
```

The same way the other 2 tasks can be launched.


```
spark-submit \
--master=local[*] \
--class com.epam.bigdata.training.spark.hotels.Task1HotelsApplication \
--conf spark.default.parallelism=8 \
--conf spark.driver.memory=1G \
--conf spark.executor.memory=2G \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.memory.fraction=0.7 \
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=1G \
--conf spark.sql.shuffle.partitions=8 \
hotels-2.11.jar \
/user/admin/expedia_recommendations/test.csv \
/user/admin/expedia_recommendations/task-1-output-10
```