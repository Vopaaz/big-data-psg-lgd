package Mongo;

import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.LinkedHashMap;
import java.util.Map;

public class MongoConfig {
    private String path;
    private LinkedHashMap<String, Object> config;

    public MongoConfig(String path) {
        this.path = path;
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

    public String getMongoRankedMatchCollectionName() {
        return getConfigItem("MongoDB", "collection", "ranked-collections");
    }
    public String getMongoPublicMatchCollectionName() {
        return getConfigItem("MongoDB", "collection", "public-collections");
    }
    public String getMongoProfessionalMatchCollectionName() {
        return getConfigItem("MongoDB", "collection", "professional-collections");
    }

    public String getMongoMatchDetailsCollectionName() {
        return getConfigItem("MongoDB", "collection", "match-result-collections");
    }

}