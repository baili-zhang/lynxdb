package zbl.moonlight.cache.server.config;

import lombok.Data;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.util.LinkedHashMap;

@Data
public class Configuration {
    private LinkedHashMap config;
    private LinkedHashMap server;
    private int port;
    private String host;

    public Configuration() {
        Yaml yaml = new Yaml();
        String path = System.getProperty("user.dir") + "/config/application.yml";
        System.out.println(path);

        try {
            FileReader fileReader = new FileReader(path);
            config = yaml.load(fileReader);
        } catch (Exception e) {
            e.printStackTrace();
        }

        server = (LinkedHashMap) config.get("server");
        host = (String) server.get("host");
        port = (int) server.get("port");
    }
}
