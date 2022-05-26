package zbl.moonlight.server.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.socket.client.ServerNode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;

public class Configuration {
    private static final Logger logger = LogManager.getLogger("Configuration");

    private static final String DEFAULT_DIR_PATH = System.getProperty("user.dir") + "/config";
    private static final String DEFAULT_FILENAME = "app.cfg";

    private static final String SEPARATOR = "=";

    private static final String HOST = "host";
    private static final String PORT = "port";

    private final ServerNode currentNode;

    private String host;
    private int port;

    public Configuration() throws IOException {
        this(DEFAULT_DIR_PATH, DEFAULT_FILENAME);
    }

    public Configuration(String dirname, String filename) throws IOException {
        Path path = Path.of(dirname, filename);
        File configFile = path.toFile();
        if(!configFile.exists()) {
            throw new RuntimeException("\"moonlight.config\" file is not existed.");
        }

        BufferedReader reader = new BufferedReader(new FileReader(configFile));

        String line;
        while((line = reader.readLine()) != null) {
            String[] item = line.trim().split(SEPARATOR);
            if(item.length != 2) {
                String message = String.format("Value of \"%s\" can not contain \"=\".", item[0].trim());
                throw new RuntimeException(message);
            }

            switch (item[0].trim().toLowerCase()) {
                case HOST -> host = item[1].trim();
                case PORT -> port = Integer.parseInt(item[1].trim());
            }
        }

        currentNode = new ServerNode(host, port);
    }

    public ServerNode currentNode() {
        return currentNode;
    }
}
