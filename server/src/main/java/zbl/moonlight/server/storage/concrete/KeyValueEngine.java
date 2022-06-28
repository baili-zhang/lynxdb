package zbl.moonlight.server.storage.concrete;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.storage.core.KeyValueStorable;
import zbl.moonlight.server.storage.query.KvDeleteQuery;
import zbl.moonlight.server.storage.query.KvGetQuery;
import zbl.moonlight.server.storage.query.KvSetQuery;
import zbl.moonlight.server.storage.query.ResultSet;

/**
 * TODO: 哪些资源需要释放？
 */
public class KeyValueEngine implements KeyValueStorable {
    private final String dataDir;

    static {
        RocksDB.loadLibrary();
    }

    KeyValueEngine() {
        Configuration config = Configuration.getInstance();
        dataDir = config.dataDir();
    }

    @Override
    public String dataDir() {
        return dataDir;
    }

    @Override
    public ResultSet kvGet(KvGetQuery query) {
        try {
            RocksDB db = RocksDB.open(path(query.database()));
            byte[] value = db.get(query.key());
            return new ResultSet(value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResultSet kvSet(KvSetQuery query) {
        try {
            RocksDB db = RocksDB.open(path(query.database()));
            db.put(query.key(), query.value());
            return new ResultSet(null);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResultSet kvDelete(KvDeleteQuery query) {
        try {
            RocksDB db = RocksDB.open(path(query.database()));
            db.delete(query.key());
            return new ResultSet(null);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
