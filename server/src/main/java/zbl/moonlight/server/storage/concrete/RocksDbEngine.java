package zbl.moonlight.server.storage.concrete;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.storage.EngineInterface;

public class RocksDbEngine implements EngineInterface {
    private static final RocksDB db;

    static {
        RocksDB.loadLibrary();

        try {
            Configuration config = Configuration.getInstance();
            db = RocksDB.open(config.dataDir());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(byte[] key, byte[] value) {
        try {
            db.put(key,value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(byte[] key) {
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
