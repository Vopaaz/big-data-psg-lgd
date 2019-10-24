package Neo4j

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Values.parameters;

object Mongo2NeoLoader {
  def main(args: Array[String]) {
    val driver = GraphDatabase
      .driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123456"));
    val session = driver.session()
    session.run("CREATE (a:Person {name: $name})", parameters("name", "Hello World"));
    session.close()
    driver.close()
  }
}
