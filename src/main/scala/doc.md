## Prerequisite
1.  Java8
2.  Spark
3.  MongoDB
4.  sbt

## Usage
#### compile
```bash
sbt
compile
```
#### Running
```bash
run
```

## Examples for spark-submit
/usr/local/spark/bin/spark-submit \
  --class SimpleApp \
  --master local[4] \
  ./target/scala-2.12/big-data-psg-lgd_2.12-0.1.0-SNAPSHOT.jar


about how to write spark-submit
./bin/spark-submit \
  --class <main-class>
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]