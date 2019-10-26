unmanagedSourceDirectories in Compile := (scalaSource in Compile).value :: Nil

scalaVersion := "2.11.12"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.7.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.4",
  "org.yaml" % "snakeyaml" % "1.25"
"org.neo4j.driver"  % "neo4j-java-driver"      % "1.7.5",
)
