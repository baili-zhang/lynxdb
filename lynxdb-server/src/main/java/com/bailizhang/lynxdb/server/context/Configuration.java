package com.bailizhang.lynxdb.server.context;

import com.bailizhang.lynxdb.core.utils.FieldUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.ReflectionUtils;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Configuration {

    public interface Default {
        String CLUSTER = "cluster";
        String SINGLE = "single";

        String LEADER = "leader";

        String USER_DIR = System.getProperty("user.dir");

        String FILENAME = "app.cfg";

        String CONFIG_DIR = USER_DIR + "/config";
        String DATA_DIR = USER_DIR + "/data/base";
        String TIMEOUT_DIR = USER_DIR + "/data/timeout";
        String RAFT_DIR = USER_DIR + "/data/raft";
        String BASE_DIR = "[base]";

        String SEPARATOR = "=";
    }

    // 反射修改 final 字段后读取时还是初始值，因为 final 字段被内联优化了
    // return runningMode;  => return "single";

    private String host = "127.0.0.1";
    private int port = 7820;
    private int messagePort = 7263;

    private String dataDir = Default.DATA_DIR;
    private String timeoutDir = Default.TIMEOUT_DIR;

    private String runningMode = Default.SINGLE;
    private String electionMode = Default.LEADER;
    private String raftLogDir = Default.RAFT_DIR;

    // TODO
    private final Charset charset = StandardCharsets.UTF_8;

    private static class Holder {
        private static final Configuration instance;

        static {
            try {
                instance = new Configuration();

                FileUtils.createDirIfNotExisted(Default.CONFIG_DIR);
                File configFile = FileUtils.createFileIfNotExisted(
                        Default.CONFIG_DIR,
                        Default.FILENAME
                );

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
    }

    public static Configuration getInstance() {
        return Holder.instance;
    }

    private Configuration() {
    }


    public ServerNode currentNode() {
        return new ServerNode(host, port);
    }
    public int messagePort() {
        return messagePort;
    }

    public String dataDir() {
        return dataDir;
    }

    public String timeoutDir() {
        return timeoutDir;
    }

    public String electionMode() {
        return electionMode;
    }

    public Charset charset() {
        return charset;
    }

    public String raftLogDir() {
        return raftLogDir;
    }

    public String runningMode() {
        return runningMode;
    }

    @Override
    public String toString() {
        Field[] fields = this.getClass().getDeclaredFields();

        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("\n");

        List<String> items = new ArrayList<>();

        for(Field field : fields) {
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
}
