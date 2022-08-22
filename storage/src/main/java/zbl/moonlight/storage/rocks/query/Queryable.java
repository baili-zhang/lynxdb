package zbl.moonlight.storage.rocks.query;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public interface Queryable {
    void doQuery(RocksDB db) throws RocksDBException;
}
