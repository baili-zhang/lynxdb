package zbl.moonlight.storage.rocks.query.table;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.query.Query;

public class TableSingleCreateColumnQuery extends Query<byte[], Void> {
    public TableSingleCreateColumnQuery(byte[] queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        db.createColumnFamily(new ColumnFamilyDescriptor(queryData));
    }
}
