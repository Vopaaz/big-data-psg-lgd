package Neo4j;

import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;

public class Neo4jConfig {
    private String path;
    private LinkedHashMap<String, Object> config;

    public Neo4jConfig(String path) {
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

    public String getHost() {
        return getConfigItem("Neo4j", "host");
    }

    public String getPort() {
        return getConfigItem("Neo4j", "port").toString();
    }

    public String getUsername() {
        return getConfigItem("Neo4j", "username").toString();
    }

    public String getPassword(){
        return getConfigItem("Neo4j", "password").toString();
    }
}
