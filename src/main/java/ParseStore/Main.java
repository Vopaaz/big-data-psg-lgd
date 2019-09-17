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

public class Main {
    public static void main(String[] args) {
        try {
            String file = args[0];

            Config conf = new Config();
            MongoClient mongoClient = MongoClients
                    .create(String.format("mongodb://%s:%d", conf.getMongoHost(), conf.getMongoPort()));
            MongoDatabase database = mongoClient.getDatabase(conf.getMongoDatabaseName());
            MongoCollection<Document> collection = database.getCollection(conf.getMongoReplayCollectionName());

            Document document = null;

            document = new Document("matchid", "test");
            document.append("combatlog", new Combatlog().getEvents(file));
            document.append("info", 1);
            document.append("lifestate", 2);
            document.append("matchend", 3);
            collection.insertOne(document);



        } catch (Exception e) {
            e.printStackTrace();
        }
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