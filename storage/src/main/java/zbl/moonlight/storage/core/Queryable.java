package zbl.moonlight.storage.core;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public interface Queryable {
    void doQuery(RocksDB db, ResultSet resultSet) throws RocksDBException;
}
