## Prerequisite
1.  Java8
2.  Maven
3.  MongoDB

## Pipeline
1.  Given a starting sequence num and the number of matches I want to get.
2.  Send get request to the Valve's API, with a starting sequence number and batch size, to get an JSON array for
matches details.
3.  Extract the last match's match sequence I get from the last call to Valve's API, as the parameter for the next call
to the Valve's API.
4.  Iterate all the match details, if we find a professional game, query on opendota's API for the information to
construct a url to download it's replay.
5.  Download the replay using the url in step 4, parse it, extract what we need and store it to mongoDB.
6.  Save all the match details we get from step 3 to mongoDB.
7.  Log how many matches we get so far.
8.  Go to step 2 if we don't have enough matches specified in step 1.

## MongoDB
Mongo DB should have a database named Dota2 with four collections: publicgames, rankedgames, professionalgames, and
matchresults.

## Usage
#### Packaging
```bash
mvn -P FetchStore package
```
#### Running
```bash
java -jar target/ParseStore.one-jar.jar start_sequence_num total_num batch_size
```
You can check logs to see what's the next the starting sequence number
## Saved Files
1.  Logs will be in directory `./logs`
2.  Downloaded replays will be in directory `./test-data/replays/zipped`
3.  Uncompressed replays will be in directory `./test-data/replays/unzipped`
4.  JSON files of match results will be in directory `./test-data/match-details`

## Data Specification
1.  Logs contain all the logging information equal or higher than 'info' level.
2.  JSON files of match result are the raw result from
 API: https://wiki.teamfortress.com/wiki/WebAPI/GetMatchHistoryBySequenceNum
3.  Replays are binary files that can be executed by the Dota2's client. When we meet a professional game, we download its
replay, and download a random public match and a rank match too.
4.  Parsed data should be refer to:
https://github.com/Vopaaz/big-data-psg-lgd/blob/master/src/main/java/ParseStore/doc.md

## Error Handling
When we encounter an error calling [valve's API](https://wiki.teamfortress.com/wiki/WebAPI/GetMatchHistoryBySequenceNum).
We increase the starting sequence number and continue.<br>
When we encounter a downloading error, we skip the current downloading task. <br>
When we can't connect to the database, we crash our program.