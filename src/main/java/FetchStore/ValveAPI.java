package FetchStore;
import java.io.BufferedReader;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.io.*;
import java.util.*;

import ParseReplay.ParseReplayExecutor;
import org.apache.commons.compress.compressors.bzip2.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.io.*;
import org.json.*;
import Mongo.MongoConfig;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ValveAPI {

    private final String USER_AGENT = "Mozilla/5.0";
    private final String AUTHORIZE_KEY = "84D40EBACB89D8276076213A092A553C";
    private final String GET_MATCH_DETAILS =
            "http://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/v1";
    private final String GET_MATCH_HISTORY =
            "http://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/v1";
    private final String GET_MATCH_HISTORY_BY_SEQUENCE_NUM =
            "http://api.steampowered.com/IDOTA2Match_570/GetMatchHistoryBySequenceNum/v1";
    private final String version = "v0.1.0";
    private final String replaysZippedDirPrefix = "./test-data/replays/zipped/";
    private final String replaysZippedDirSuffix = ".dem.bz2";
    private final String professionalGames = "professional/";
    private final String publicGames = "public/";
    private final String rankedGames = "ranked/";
    private final String replaysUnzippedDirPrefix = "./test-data/replays/unzipped/";
    private final String replaysUnzippedDirSuffix = ".dem";
    private final String jsonPrefix = "./test-data/match-details/";
    private final String jsonSuffix = ".json";
    private int publicGamesNum;
    private int rankedGamesNum;

    MongoConfig conf;
    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> publicCollection;
    MongoCollection<Document> rankedCollection;
    MongoCollection<Document> professionalCollection;
    MongoCollection<Document> matchesCollection;
    ParseReplayExecutor parser;
    OpendotaAPI opendotaAPI;
    Logger logger;

    public ValveAPI(String configPath) {
        logger = LoggerFactory.getLogger(ValveAPI.class);
        conf = new MongoConfig(configPath);
        mongoClient = MongoClients
                .create(String.format("mongodb://%s:%d", conf.getMongoHost(), conf.getMongoPort()));
        database = mongoClient.getDatabase(conf.getMongoDatabaseName());
        publicCollection = database.getCollection(conf.getMongoPublicMatchCollectionName());
        rankedCollection = database.getCollection(conf.getMongoRankedMatchCollectionName());
        professionalCollection = database.getCollection(conf.getMongoProfessionalMatchCollectionName());
        matchesCollection = database.getCollection(conf.getMongoMatchDetailsCollectionName());
        opendotaAPI = new OpendotaAPI();
        publicGamesNum = 0;
        rankedGamesNum = 0;
        parser = new ParseReplayExecutor();
    }

    public boolean uncompressBz2(String source, String target) {
        try {
            source = FilenameUtils.separatorsToSystem(source);
            target = FilenameUtils.separatorsToSystem(target);
            FileInputStream in = new FileInputStream(source);
            FileOutputStream out = new FileOutputStream(target);
            BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(in);
            final byte[] buffer = new byte[4096];
            int n = 0;
            while (-1 != (n = bzIn.read(buffer))) {
                out.write(buffer, 0, n);
            }
            out.close();
            bzIn.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private void writeMultipleMatchesToDB(JSONArray matches) throws Exception {
        for(int i = 0; i < matches.length(); ++i) {
            Date start = new Date();
            JSONObject curMatch = matches.getJSONObject(i);
            if(curMatch.getInt("leagueid") != 0) {
                List<Long> list = new ArrayList<>();
                list.add(curMatch.getLong("match_id"));
                publicGamesNum++;
                rankedGamesNum++;
                Runnable downloader = new RunDownload(list, professionalGames);
                Thread downloadTask = new Thread(downloader);
                downloadTask.start();
            }
            if(publicGamesNum != 0 && curMatch.getInt("lobby_type") == 0 && checkValidGame(curMatch)) {
                List<Long> list = new ArrayList<>();
                list.add(curMatch.getLong("match_id"));
                publicGamesNum--;
                Runnable downloader = new RunDownload(list, publicGames);
                Thread downloadTask = new Thread(downloader);
                downloadTask.start();
            }
            if(rankedGamesNum != 0 && curMatch.getInt("lobby_type") == 7 && checkValidGame(curMatch)) {
                List<Long> list = new ArrayList<>();
                list.add(curMatch.getLong("match_id"));
                rankedGamesNum--;
                Runnable downloader = new RunDownload(list, rankedGames);
                Thread downloadTask = new Thread(downloader);
                downloadTask.start();
            }
            writeDetailsToDB(curMatch, start);
        }
    }

    private boolean checkValidGame(JSONObject match) {
        JSONArray playerStatus = match.getJSONArray("players");
        int len = playerStatus.length();
        if(len != 10) return false;
        for(int i = 0; i < len; ++i) {
            JSONObject curPlayer = playerStatus.getJSONObject(i);
            if(curPlayer.getInt("leaver_status") != 0) return false;
        }
        return true;
    }

//    private void downloadRepByMatchID(List<Long> matches, String directory) throws Exception {
//        List<String> urls = opendotaAPI.getRepInfo(matches);
//        for(int i = 0; i < matches.size(); ++i) {
//            String zippedDir = replaysZippedDirPrefix + directory + matches.get(i) + replaysZippedDirSuffix;
//            String unzippedDir = replaysUnzippedDirPrefix + directory + matches.get(i) + replaysUnzippedDirSuffix;
//            if(downloadURL(urls.get(i), zippedDir)) {
//                if(uncompressBz2(zippedDir, unzippedDir)) {
//                    if(directory.equals(publicGames)) {
//                        logger.info("One public matching game was downloaded.");
//                        logger.info("Start parsing it and save the result to db");
//                        insertDocumentToDB(parser.getReplayInfoDocument(unzippedDir, matches.get(i)), publicCollection);
//                        publicGamesNum--;
//                    }
//                    else if(directory.equals(rankedGames)) {
//                        logger.info("One ranked game was downloaded.");
//                        logger.info("Start parsing it and save the result to db");
//                        insertDocumentToDB(parser.getReplayInfoDocument(unzippedDir, matches.get(i)), rankedCollection);
//                        rankedGamesNum--;
//                    }
//                    else {
//                        logger.info("One professional game was downloaded.");
//                        logger.info("Start parsing it and save the result to db");
//                        insertDocumentToDB(parser.getReplayInfoDocument(unzippedDir, matches.get(i)), professionalCollection);
//                        publicGamesNum++;
//                        rankedGamesNum++;
//                    }
//                }
//            }
//        }
//    }

    public void writeDetailsToDB(JSONObject match, Date start) throws Exception{
        Document document = Document.parse(match.toString());
        Date end = new Date();
        ArrayList<HashMap<String, Object>> provenanceList = new ArrayList<HashMap<String, Object>>();
        HashMap<String, Object> provenance = new HashMap<String, Object>();
        provenance.put("name", "Extract match details from a batch of matches and save it to DB");
        provenance.put("code", "FetchStore.ValveAPI.writeDetailsToDB");
        provenance.put("version", version);
        provenance.put("start", start);
        provenance.put("end", end);
        provenance.put("elapsed_ms", end.getTime() - start.getTime());
        provenanceList.add(provenance);
        document.append("provenance", provenanceList);
        matchesCollection.insertOne(document);
    }

    private void insertDocumentToDB(Document document, MongoCollection<Document> collection) {
        collection.insertOne(document);
    }
    public static void main(String[] args) throws Exception {
        ValveAPI api = new ValveAPI("config.yml");
        api.getMultipleMatchesBySeqNum("4182489531", 20, 5);
    }


    private boolean downloadURL(String url, String fileTarget) {
        try {
            logger.info("Start downloading {} to address {}.", url, fileTarget);
            URL obj = new URL(url);
            File target = new File(fileTarget);
            FileUtils.copyURLToFile(obj, target);
            logger.info("Successfully downloaded {} to address {}.", url, fileTarget);
        }
        catch (Exception e) {
            logger.error("Failed to download {} to address {}.", url, fileTarget);
            logger.error("Skip the file from URL {}.", url);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public String getMatchesBySeqNum(String seqNum, int num) {
        try {
            String numString = Integer.toString(num);
            logger.info("Start getting matches through Valve's API.");
            logger.info("Matches sequence numbers start by {}, and with total number {}", seqNum, num);
            List<String> fields = new ArrayList<>();

            fields.add("key");
            fields.add(AUTHORIZE_KEY);
            fields.add("start_at_match_seq_num");
            fields.add(seqNum);
            fields.add("matches_requested");
            fields.add(numString);

            String url = constructURL(fields, GET_MATCH_HISTORY_BY_SEQUENCE_NUM);
            String res = sendGetRequest(url);
            return res;
        }
        catch (Exception e) {
            e.printStackTrace();
            logger.error("Failed to get matches sequence numbers start by {}, and with total number {}", seqNum, num);
            return null;
        }
    }

    public void getMultipleMatchesBySeqNum(String startSeqNum, int totalNum, int batchSize) throws Exception {
        logger.info("Starting a new task.");
        logger.info("Get {} matches with batch size of {} and sequence number start by {}.", totalNum, batchSize, startSeqNum);
        Date start = new Date();
        int sofar = 0;
        String curStartSeqNum = startSeqNum;
        while(sofar < totalNum) {
            logger.info("Starting to get a batch start with sequence number {}.", curStartSeqNum);
            String seqResult = getMatchesBySeqNum(curStartSeqNum, batchSize);
            if(seqResult == null) {
                logger.error("Failed to get batch start with sequence number {}", curStartSeqNum);
                logger.error("Skip the current sequence number");
                long nextSeqNum = Long.parseLong(curStartSeqNum);
                curStartSeqNum = Long.toString(++nextSeqNum);
                continue;
            }
            logger.info("Successfully get a batch start with sequence number {}.", curStartSeqNum);
            JSONObject jsonResult = new JSONObject(seqResult);
            JSONArray matches = jsonResult.getJSONObject("result").getJSONArray("matches");

            logger.info("Trying to write the previous result to database");
            writeMultipleMatchesToDB(matches);

            logger.info("Successfully writing a batch start with sequence number {} to database.", curStartSeqNum);
            sofar += matches.length();

            if(matches.length() == 0) {
                logger.error("Current sequence number is larger than the sequence number of every match");
                Date end = new Date();
                logger.error("Can't continue to get the data.");
                logger.error("The task was closed unsuccessfully in {} seconds.", (end.getTime() - start.getTime()) / 1000.0);
                return;
            }

            long nextSeqNum = matches.getJSONObject(matches.length() - 1).getLong("match_seq_num");
            String fileName = curStartSeqNum + "_to_" + nextSeqNum;

            logger.info("Trying to save the previous result as a JSON file");
            writeJSONToFile(jsonResult, fileName, jsonPrefix, jsonSuffix);

            logger.info("Successfully writing to a local JSON file");
            curStartSeqNum = Long.toString(++nextSeqNum);
            logger.info("Next batch starting sequence is {}.", curStartSeqNum);
        }
        Date end = new Date();
        logger.info("Successfully complete the task with start sequence number {}, and total number {}.", startSeqNum, totalNum);
        logger.info("Next batch should start with sequence number: {}.", curStartSeqNum);
        logger.info("The task was done in {} seconds.", (end.getTime() - start.getTime()) / 1000.0);
    }

    private void writeJSONToFile(JSONObject obj, String fileName, String filePrefix, String fileSuffix) {
        String path = filePrefix + fileName + fileSuffix;
        try {
            logger.info("Writing to file {}.", path);
            File targetFile = new File(path);
            FileUtils.writeStringToFile(targetFile, obj.toString(), "US-ASCII");
        }
        catch (Exception e) {
            logger.error("Failed to write to file {}.", path);
            e.printStackTrace();
        }
    }

    public void getRecentMatches(String reqNum) throws Exception {
        List<String> fields = new ArrayList<>();
        fields.add("key");
        fields.add(AUTHORIZE_KEY);
        fields.add("matches_requested");
        fields.add(reqNum);

        String url = constructURL(fields, GET_MATCH_HISTORY);

        String res = sendGetRequest(url);

        System.out.println(res);

    }

    private String sendGetRequest(String url) throws Exception {
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        //add request header
        con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = con.getResponseCode();
        logger.info("Sending 'GET' request to URL {}.", url);
        logger.info("Response Code : {}", responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }

        in.close();
        String res = response.toString();
        logger.info("Successfully get the result from URL {}.", url);
        return res;
    }

    public String constructURL(List<String> fields, String api) {
        if(fields.size() == 0) return api;
        StringBuilder sb = new StringBuilder(api);
        sb.append('?');
        for(int i = 0; i < fields.size(); i += 2) {
            if(i != 0) sb.append("&");
            sb.append(fields.get(i));
            sb.append('=');
            sb.append(fields.get(i + 1));
        }

        return sb.toString();
    }

    private class RunDownload implements Runnable {
        List<Long> matches;
        String directory;
        public RunDownload(List<Long> matches, String directory) {
            this.matches = matches;
            this.directory = directory;
        }

        public void run() {
            downloadRepByMatchID(matches, directory);
        }

        private void downloadRepByMatchID(List<Long> matches, String directory) {
            try {
                List<String> urls = opendotaAPI.getRepInfo(matches);
                for (int i = 0; i < matches.size(); ++i) {
                    String zippedDir = replaysZippedDirPrefix + directory + matches.get(i) + replaysZippedDirSuffix;
                    String unzippedDir = replaysUnzippedDirPrefix + directory + matches.get(i) + replaysUnzippedDirSuffix;
                    if (downloadURL(urls.get(i), zippedDir)) {
                        if (uncompressBz2(zippedDir, unzippedDir)) {
                            if (directory.equals(publicGames)) {
                                logger.info("One public matching game was downloaded.");
                                logger.info("Start parsing it and save the result to db");
                                insertDocumentToDB(parser.getReplayInfoDocument(unzippedDir, matches.get(i)), publicCollection);
                            } else if (directory.equals(rankedGames)) {
                                logger.info("One ranked game was downloaded.");
                                logger.info("Start parsing it and save the result to db");
                                insertDocumentToDB(parser.getReplayInfoDocument(unzippedDir, matches.get(i)), rankedCollection);
                            } else {
                                logger.info("One professional game was downloaded.");
                                logger.info("Start parsing it and save the result to db");
                                insertDocumentToDB(parser.getReplayInfoDocument(unzippedDir, matches.get(i)), professionalCollection);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                if (directory.equals(publicGames)) {
                    publicGamesNum++;
                } else if (directory.equals(rankedGames)) {
                    rankedGamesNum++;
                } else {
                    publicGamesNum--;
                    rankedGamesNum--;
                }
            }
        }
    }
}
