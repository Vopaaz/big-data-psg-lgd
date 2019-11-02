# Big Data - PSG.LGD

Dota 2 big data analysis group project for Big Data Engineering and Architecture, CSCI 5751, UMN.

## Documentations

- [Proposal](/reports/proof-of-concept/proposal.pdf)
- [Report](/reports/proof-of-concept/write-up.pdf)

## Environment Setup

### Prerequisites

- Java8
- MongoDB
- Spark
- Neo4j
- sbt

### Configuration

Create a `config.yml` at the project root. You can refer to the [example](config.example.yml).

## Running

### ETL

```bash
mvn -P FetchStore package
java -jar target/FetchStore.one-jar.jar <start_sequence_num> <total_num> <batch_size>
```

You can test by setting the sequence number to `4182489531`, which is a very famous professional dota2 game.

For more detail, you can refer to the ETL pipeline [documentation](src/main/java/FetchStore/doc.md).

### Analysis

```bash
sbt
run
```

It will ask you to choose from

1. BadManner
2. CostTime
3. DamageRate
4. EcoDistributionTrain
5. First15minGain
6. FirstBloodPredict
7. FirstBloodTrain
8. HeroMostStats
9. MostBan
10. MostPick
11. MostPurchasedItem
12. MostUsedItem
13. Neo4j.Mongo2NeoLoader
14. TeamBattleDetector

Each one answers one of our business question, except for 13. `Neo4j.Mongo2NeoLoader` is used for loading data from MongoDB to Neo4j.
