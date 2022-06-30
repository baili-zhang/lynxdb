package zbl.moonlight.server.storage.core;

import org.rocksdb.RocksDB;

public interface Queryable {
    void doQuery(RocksDB db, ResultSet resultSet);
}
