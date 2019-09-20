package FetchStore;
import java.io.BufferedReader;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.io.*;
import java.util.*;

import com.sun.tools.corba.se.idl.StringGen;
import org.apache.commons.compress.compressors.bzip2.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.io.*;
import org.json.*;
import Mongo.MongoConfig;
import org.bson.Document;

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
    private final String replaysUnzippedDirPrefix = "./test-data/replays/unzipped/";
    private final String replaysUnzippedDirSuffix = ".dem";
    MongoConfig conf;
    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> repCollection;
    MongoCollection<Document> matchCollection;
    OpendotaAPI opendotaAPI;

    public ValveAPI(String configPath) {
        conf = new MongoConfig(configPath);
        mongoClient = MongoClients
                .create(String.format("mongodb://%s:%d", conf.getMongoHost(), conf.getMongoPort()));
        database = mongoClient.getDatabase(conf.getMongoDatabaseName());
        matchCollection = database.getCollection(conf.getMongoMatchDetailsCollectionName());
        repCollection = database.getCollection(conf.getMongoReplayCollectionName());
        opendotaAPI = new OpendotaAPI();
    }

    public boolean uncompressBz2(String source, String target) {
        try {
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
            return false;
        }
        return true;
    }

    private void writeMultipleMatchesToDB(JSONArray matches) throws Exception {
        List<String> toDownload = new ArrayList<>();
        for(int i = 0; i < matches.length(); ++i) {
            Date start = new Date();
            JSONObject curMatch = matches.getJSONObject(i);
            if(curMatch.getInt("leagueid") != 0) toDownload.add(Long.toString(curMatch.getLong("match_id")));
            writeDetailsToDB(curMatch, start);
        }
        downloadRepByMatchID(toDownload);
    }

    private void downloadRepByMatchID(List<String> proMatches) throws Exception {
        List<String> urls = opendotaAPI.getRepInfo(proMatches);
        for(int i = 0; i < proMatches.size(); ++i) {
            String zippedDir = replaysZippedDirPrefix + proMatches + replaysZippedDirSuffix;
            String unzippedDir = replaysUnzippedDirPrefix + proMatches + replaysUnzippedDirSuffix;
            downloadURL(urls.get(i), zippedDir);
            uncompressBz2(zippedDir, unzippedDir);
        }
    }

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
        matchCollection.insertOne(document);
    }

    public static void main(String[] args) throws Exception {
        ValveAPI api = new ValveAPI("config.yml");
        api.getMultipleMatchesBySeqNum("4182489531", 20, 5);
//        api.getRecentMatches("10");
//        api.getMatchesBySeqNum("4218902824", 20);
//        List<String> test = new ArrayList<>();
//        test.add("4986461644");
//        test.add("4986362254");
//        test.add("4986260666");
//        List<String> repUrls = api1.getRepInfo(test);
//        int curIndex = 1;
//        for(String s: repUrls){
//            System.out.println("Downloading: " + s);
//            api.downloadURL(s, "./test-data/replays/" + (curIndex++) + ".dem.bz2");
//        }
//        api.uncompressBz2("./test-data/replays/1.dem.bz2", "./test-data/replays/1.dem");
//        api.writeDetailsToDB("4986461644");
    }


    private void downloadURL(String url, String fileTarget) throws Exception{
        URL obj = new URL(url);
        File target = new File(fileTarget);
        FileUtils.copyURLToFile(obj, target);
    }

    public String getMatchesBySeqNum(String seqNum, int num) throws Exception{
        String numString = Integer.toString(num);

        List<String> fields = new ArrayList<>();

        fields.add("key");
        fields.add(AUTHORIZE_KEY);
        fields.add("start_at_match_seq_num");
        fields.add(seqNum);
        fields.add("matches_requested");
        fields.add(numString);

        String url = constructURL(fields, GET_MATCH_HISTORY_BY_SEQUENCE_NUM);

        String res = sendGetRequest(url);
//        JSONObject obj = new JSONObject(res);
//        System.out.println(obj.getJSONObject("result").toString());
        System.out.println(res);
        return res;
    }

    public void getMultipleMatchesBySeqNum(String startSeqNum, int totalNum, int batchSize) throws Exception {
        int sofar = 0;
        String lastStartSeqNum = "";
        String curStartSeqNum = startSeqNum;
        while(sofar < totalNum && !lastStartSeqNum.equals(curStartSeqNum)) {
            String seqResult = getMatchesBySeqNum(curStartSeqNum, batchSize);
            JSONObject jsonResult = new JSONObject(seqResult);
            JSONArray matches = jsonResult.getJSONObject("result").getJSONArray("matches");
            writeMultipleMatchesToDB(matches);
            sofar += matches.length();
            lastStartSeqNum = curStartSeqNum;
            long nextSeqNum = matches.getJSONObject(matches.length() - 1).getLong("match_seq_num");
            curStartSeqNum = Long.toString(++nextSeqNum);
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
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }

        in.close();
        //  print result
        String res = response.toString();
//        System.out.println(res);
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
}
