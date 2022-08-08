package zbl.moonlight.storage.rocks.query.table;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.query.Query;

import java.util.List;
import java.util.stream.Collectors;

public class TableBatchCreateColumnQuery extends Query<List<byte[]>, Void> {
    public TableBatchCreateColumnQuery(List<byte[]> queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = queryData.stream()
                .map(ColumnFamilyDescriptor::new)
                .collect(Collectors.toList());
        db.createColumnFamilies(columnFamilyDescriptors);
    }
}
