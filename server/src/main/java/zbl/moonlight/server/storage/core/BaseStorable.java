package zbl.moonlight.server.storage.core;

import java.nio.file.Path;

public interface BaseStorable {
    String dataDir();

    default String path(String database) {
        return Path.of(dataDir(), database).toString();
    }
}
