package com.bailizhang.lynxdb.server.context;

import com.bailizhang.lynxdb.core.utils.FieldUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Configuration {
    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    public interface Default {
        String CLUSTER                  = "cluster";
        String SINGLE                   = "single";

        String USER_DIR                 = System.getProperty("user.dir");

        String FILENAME                 = "app.cfg";

        String HOST                     = "127.0.0.1";
        String PORT                     = "7820";

        String CONFIG_DIR               = USER_DIR + "/config";
        String DATA_DIR                 = USER_DIR + "/data/base";
        String RAFT_LOGS_DIR            = USER_DIR + "/data/raft/logs";
        String RAFT_META_DIR            = USER_DIR + "/data/raft/meta";

        String BASE_DIR                 = "[base]";

        String SEPARATOR                = "=";

        String TRUE                     = "true";
    }

    // 反射修改 final 字段后读取时还是初始值，因为 final 字段被内联优化了
    // return runningMode;  => return "single";

    private String host;
    private String port;

    private String dataDir;
    private String raftLogsDir;
    private String raftMetaDir;

    private String runningMode;
    private String initClusterMembers;
    private String enableFlightRecorder;

    // TODO
    private final Charset charset   = StandardCharsets.UTF_8;

    private static class Holder {
        private static final Configuration instance;

        static {
            try {
                String filename = configFilename();

                logger.info("Config file name: {}", filename);

                FileUtils.createDirIfNotExisted(Default.CONFIG_DIR);
                File configFile = FileUtils.createFileIfNotExisted(
                        Default.CONFIG_DIR,
                        filename
                );

                instance = new Configuration();
                instance.initDefaultValue();

                BufferedReader reader = new BufferedReader(new FileReader(configFile));

                String line;
                while((line = reader.readLine()) != null) {
                    String[] item = line.trim().split(Default.SEPARATOR);
                    if(item.length != 2) {
                        String message = String.format("Value of \"%s\" can not contain \"=\".", item[0].trim());
                        throw new RuntimeException(message);
                    }

                    String key = item[0].trim();
                    String value = item[1].trim();

                    value = value.replace(Default.BASE_DIR, Default.USER_DIR);

                    FieldUtils.set(instance, key, value);
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static String configFilename() {
            String filename = System.getProperty("lynxdb.config.filename");

            if(filename == null) {
                return Default.FILENAME;
            }

            return filename;
        }
    }

    public static Configuration getInstance() {
        return Holder.instance;
    }

    private Configuration() {
    }


    public ServerNode currentNode() {
        return new ServerNode(host, Integer.parseInt(port));
    }

    public String dataDir() {
        return dataDir;
    }

    public String initClusterMembers() {
        return initClusterMembers;
    }

    public Charset charset() {
        return charset;
    }

    public String raftLogsDir() {
        return raftLogsDir;
    }

    public String raftMetaDir() {
        return raftMetaDir;
    }

    public String runningMode() {
        return runningMode;
    }

    public boolean enableFlightRecorder() {
        return Default.TRUE.equals(enableFlightRecorder);
    }

    @Override
    public String toString() {
        Field[] fields = this.getClass().getDeclaredFields();

        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("\n");

        List<String> items = new ArrayList<>();

        for(Field field : fields) {
            if(Modifier.isStatic(field.getModifiers())) {
                continue;
            }

            String key = field.getName();
            Object value = FieldUtils.get(this, field);

            items.add("  " + key + ": " + value);
        }

        String body = String.join(",\n", items);

        builder.append(body);
        builder.append("\n");
        builder.append("}");

        return builder.toString();
    }

    private void initDefaultValue() {
        host = Default.HOST;
        port = Default.PORT;

        dataDir = Default.DATA_DIR;
        raftLogsDir = Default.RAFT_LOGS_DIR;
        raftMetaDir = Default.RAFT_META_DIR;

        runningMode = Default.SINGLE;
    }
}
