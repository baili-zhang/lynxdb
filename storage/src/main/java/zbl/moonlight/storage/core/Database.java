package zbl.moonlight.storage.core;

import zbl.moonlight.storage.rocks.query.Query;

import java.nio.file.Path;

public abstract class Database implements AutoCloseable {
    protected final String name;
    protected final String dataDir;

    protected Database(String name, String dataDir) {
        this.name = name;
        this.dataDir = dataDir;
    }

    protected String path() {
        return Path.of(dataDir, name).toString();
    }

    public abstract ResultSet<?> doQuery(Query<?, ?> query) throws Exception;
}
