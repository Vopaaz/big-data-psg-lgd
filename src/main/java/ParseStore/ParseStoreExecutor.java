package ParseStore;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.yaml.snakeyaml.Yaml;

public class ParseStoreExecutor {
    private final String version = "v0.1.0";

    public ParseStoreExecutor() {

    }

    public Document parseFileStoreMongo(String file, String matchId) {
        try {
            Date start = new Date();
            Document document = new Document("matchid", matchId);
            document.append("combatlog", new Combatlog().getEvents(file));
            document.append("info", new Info().getInfo(file));
            document.append("chat", new Chat().getChat(file));
            document.append("lifestate", new Lifestate().getStates(file));
            Date end = new Date();

            addProvenance(document, start, end);
            return document;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    private void addProvenance(Document document, Date start, Date end) {
        ArrayList<HashMap<String, Object>> provenanceList = new ArrayList<HashMap<String, Object>>();
        HashMap<String, Object> provenance = new HashMap<String, Object>();
        provenance.put("name", "Parse .dem file to MongoDB");
        provenance.put("code", "ParseStore.ParseStoreExecutor.parseFileStoreMongo");
        provenance.put("version", version);
        provenance.put("start", start);
        provenance.put("end", end);
        provenance.put("elapsed_ms", end.getTime() - start.getTime());
        provenanceList.add(provenance);

        document.append("provenance", provenanceList);
    }

    public String getVersion() {
        return version;
    }

}

//class Config {
//    private String path = "config.yml";
//    private LinkedHashMap<String, Object> config = null;
//
//    public Config() {
//        try {
//            config = new Yaml().load(new FileReader(path));
//
//            if (config == null) {
//                throw new FileNotFoundException();
//            }
//        } catch (FileNotFoundException e) {
//            System.out.println("config.yml not found.");
//            e.printStackTrace();
//        }
//    }
//
//    private String getConfigItem(String... tags) {
//        Map temp = config;
//        String result = null;
//        for (int i = 0; i < tags.length; i++) {
//            if (i != tags.length - 1) {
//                temp = (Map) temp.get(tags[i]);
//            } else {
//                result = temp.get(tags[i]).toString();
//            }
//        }
//        return result;
//    }
//
//    public int getMongoPort() {
//        return Integer.parseInt(getConfigItem("MongoDB", "port"));
//    }
//
//    public String getMongoHost() {
//        return getConfigItem("MongoDB", "host");
//    }
//
//    public String getMongoDatabaseName() {
//        return getConfigItem("MongoDB", "database");
//    }
//
//    public String getMongoReplayCollectionName() {
//        return getConfigItem("MongoDB", "collection", "replay-collection");
//    }
//}
