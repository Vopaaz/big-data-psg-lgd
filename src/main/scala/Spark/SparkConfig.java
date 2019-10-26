package Spark;

import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;

public class SparkConfig {
    private String path;
    private LinkedHashMap<String, Object> config;

    public SparkConfig(String path) {
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

    private List<String> getConfigItemList(String tags) {
        Map temp = config;
        return (List<String>) temp.get(tags);
    }

    public String getCollection(String type) {
        switch(type) {
            case "rankedGames":
                return getConfigItem("Spark", "rankedGames");
            case "publicGames":
                return getConfigItem("Spark", "publicGames");
            case "professionalGames":
                return getConfigItem("Spark", "professionalGames");
            case "matchResults":
                return getConfigItem("Spark", "matchResults");
            case "heros":
                return getConfigItem("Spark", "heros");
            case "items":
                return getConfigItem("Spark", "items");
            default:
                throw new IllegalArgumentException(
                    type + " is not a valid collection name"
                    );
        }
    }

    public String getAppName(String funName) {
        return getConfigItem("Spark", "name") + "+" + funName;
    }

}