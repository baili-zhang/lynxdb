package zbl.moonlight.storage.query;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.ResultSet;

import java.util.List;

public interface Queryable {
    void doQuery(RocksDB db, ResultSet resultSet) throws RocksDBException;
    List<byte[]> columnFamilies();
}
