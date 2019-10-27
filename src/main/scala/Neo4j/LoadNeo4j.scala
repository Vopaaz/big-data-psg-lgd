package Neo4j

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Values.parameters;
import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import Spark.SparkSessionCreator
import java.util.ArrayList
import scala.collection.JavaConversions._
import Neo4j.Neo4jConfig

object Mongo2NeoLoader {

  def main(args: Array[String]) {
    val spark: SparkSession = new SparkSessionCreator()
      .getSparkSession("Neo4jLoader", "matchResults", "matchResults")
    val rdd = MongoSpark.load(spark.sparkContext)

    val valid_games = rdd
      .filter(x => x.getInteger("leagueid") != 0)
      .filter(x => x.get("radiant_name") != null && x.get("dire_name") != null)

    val all_teams = valid_games
      .flatMap[String](
          x => List(x.getString("radiant_name"), x.getString("dire_name"))
      )
      .distinct()
      .collect()

    val win_lose = valid_games
      .map(
          x =>
            if (x.get("radiant_win") == true)
              Tuple2(x.getString("radiant_name"), x.getString("dire_name"))
            else
              Tuple2(x.getString("dire_name"), x.getString("radiant_name"))
      )
      .collect()

    val all_teams_map = all_teams.map(
        x => mapAsJavaMap(Map("name" -> x))
    )

    val neo_conf: Neo4jConfig = new Neo4jConfig("config.yml")
    val driver = GraphDatabase
      .driver(
          "bolt://" + neo_conf.getHost() + ":" + neo_conf.getPort(),
          AuthTokens.basic(neo_conf.getUsername, neo_conf.getPassword)
      )
    val session = driver.session()
    session.run("MATCH (n) DETACH DELETE n")
    session.run(
        """UNWIND $teams AS map
        CREATE (n:Team)
        SET n = map""",
        parameters("teams", all_teams_map)
    );
    win_lose.foreach(
        x =>
          session.run(
              """MATCH (a:Team), (b:Team)
            WHERE a.name = $winner AND b.name = $loser
            CREATE (a)-[r:Defeat]->(b)
          """,
              parameters("winner", x._1, "loser", x._2)
          )
    )
    spark.close()
    session.close()
    driver.close()
  }
}
