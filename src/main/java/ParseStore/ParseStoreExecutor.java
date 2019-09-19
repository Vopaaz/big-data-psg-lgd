package ParseStore;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.BsonArray;
import org.bson.Document;
import org.yaml.snakeyaml.Yaml;

public class ParseStoreExecutor {
    private final MongoCollection<Document> collection;

    ParseStoreExecutor(){
        Config conf = new Config();
        MongoClient mongoClient = MongoClients
                .create(String.format("mongodb://%s:%d", conf.getMongoHost(), conf.getMongoPort()));
        MongoDatabase database = mongoClient.getDatabase(conf.getMongoDatabaseName());
        collection = database.getCollection(conf.getMongoReplayCollectionName());
    }

    public void parseFileStoreMongo(String file, String matchId) throws Exception {
        Document document = new Document("matchid", matchId);
        document.append("combatlog", new Combatlog().getEvents(file));
        document.append("info", new Info().getInfo(file));
        document.append("chat", new Chat().getChat(file));
        document.append("lifestate", new Lifestate().getStates(file));
        collection.insertOne(document);
    }

}

class Config {
    private String path = "config.yml";
    private LinkedHashMap<String, Object> config = null;

    public Config() {
        try {
            config = new Yaml().load(new FileReader(path));

            if (config == null) {
                throw new FileNotFoundException();
            }
        } catch (FileNotFoundException e) {
            System.out.println("config.yml not found.");
            e.printStackTrace();
        }
    }

    private String getConfigItem(String... tags) {
        Map temp = config;
        String result = null;
        for (int i = 0; i < tags.length; i++) {
            if (i != tags.length - 1) {
                temp = (Map) temp.get(tags[i]);
            } else {
                result = temp.get(tags[i]).toString();
            }
        }
        return result;
    }

    public int getMongoPort() {
        return Integer.parseInt(getConfigItem("MongoDB", "port"));
    }

    public String getMongoHost() {
        return getConfigItem("MongoDB", "host");
    }

    public String getMongoDatabaseName() {
        return getConfigItem("MongoDB", "database");
    }

    public String getMongoReplayCollectionName() {
        return getConfigItem("MongoDB", "collection", "replay-collection");
    }
}