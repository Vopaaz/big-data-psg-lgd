package FetchStore;
import java.io.BufferedReader;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.io.*;
import java.util.*;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.io.*;
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
    MongoConfig conf;
    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> repCollection;
    MongoCollection<Document> matchCollection;

    public ValveAPI(String configPath) {
        conf = new MongoConfig(configPath);
        mongoClient = MongoClients
                .create(String.format("mongodb://%s:%d", conf.getMongoHost(), conf.getMongoPort()));
        database = mongoClient.getDatabase(conf.getMongoDatabaseName());
        matchCollection = database.getCollection(conf.getMongoMatchDetailsCollectionName());
        repCollection = database.getCollection(conf.getMongoReplayCollectionName());
    }

    public void writeDetailsToDB(String matchId) throws Exception{
        List<String> fields = new ArrayList<>();
        fields.add("key");
        fields.add(AUTHORIZE_KEY);
        fields.add("match_id");
        fields.add(matchId);

        String url = constructURL(fields, GET_MATCH_DETAILS);
        String res = sendGetRequset(url);
        Document document = Document.parse(res);
        matchCollection.insertOne(document);
    }

    public static void main(String[] args) throws Exception {
        ValveAPI api = new ValveAPI("config.yml");
        OpendotaAPI api1 = new OpendotaAPI();
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
        api.writeDetailsToDB("4986461644");
    }


    private void downloadURL(String url, String fileTarget) throws Exception{
        URL obj = new URL(url);
        File target = new File(fileTarget);
        FileUtils.copyURLToFile(obj, target);
    }

    private String sendGetRequset(String url) throws Exception {
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
        //print result
        String res = response.toString();
        System.out.println(res);
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
