Hi everyone. We are team PSG.LGD and we are presenting our Dota 2 data analysis system.

Dota 2 is a multiplayer online battle arena video game.
Lots of professional players and teams take it as their career.
Our system can provide insights to their training thus create value.

We explored two datasets.
The first one provides a summary about a game by providing the players' status at the end of it.
And the second one are game replays, which involve everything happened in the game.
Both of them are provided by an official API and we can get them by a HTTP request.

We choose MongoDB as our primary storage technology, as the format of our first dataset is JSON.
We have also find a parser for the second dataset and wrote some scripts to transform it into a Document.

The relationship between professional teams, between heros are all interesting to take a look at.
But exploring this in MongoDB may require considerable amount of joins.
Therefore we applied Neo4j, a graph database to support us.

We use Spark as our analytics engine and our system is deployed on the AWS server.
This is an overview of our system architecture.

<!-- Total time: 1:15 -->





