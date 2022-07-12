package zbl.moonlight.storage.core;

import org.rocksdb.RocksDB;
import zbl.moonlight.storage.query.Queryable;

import java.nio.file.Path;

public abstract class AbstractDatabase {
    protected final String name;
    protected final String dataDir;

    static {
        RocksDB.loadLibrary();
    }

    protected AbstractDatabase(String name, String dataDir) {
        this.name = name;
        this.dataDir = dataDir;
    }

    protected String path() {
        return Path.of(dataDir, name).toString();
    }

    public abstract ResultSet doQuery(Queryable query);
}
