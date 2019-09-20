##To Do
1.  The gap between two calls to the api?
2.  What if the times I call the api hit the limit?
3.  Logging the last match seq number so I can continue from crashing.
4.  Handling all the exceptions
5.  What about the raw data? If we have to save them, we should use cloud technology
6.  Do we need to create an account for Open Dota?

##Pipeline
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

