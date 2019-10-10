scalaVersion := "2.12.10"
mainClass in (Compile, run) := Some("example.Hello")

libraryDependencies ++= Seq(
    "com.skadistats"     % "clarity"                    % "2.4",
    "joda-time"          % "joda-time"                  % "2.7",
    "ch.qos.logback"     % "logback-classic"            % "1.1.3",
    "org.mongodb"        % "mongodb-driver-sync"        % "3.11.0",
    "org.yaml"           % "snakeyaml"                  % "1.25",
    "commons-io"         % "commons-io"                 % "2.6",
    "org.json"           % "json"                       % "20190722",
    "org.apache.commons" % "commons-compress"           % "1.19",
    "org.slf4j"          % "slf4j-api"                  % "1.7.6",
    "org.apache.spark"   % "spark-core_2.12"            % "2.4.4",
    "org.apache.spark"   % "spark-sql_2.12"             % "2.4.4",
    "org.mongodb.spark"  % "mongo-spark-connector_2.12" % "2.4.1"
)
