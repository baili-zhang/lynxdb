package zbl.moonlight.storage.rocks.query.table;

import org.rocksdb.*;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.core.exception.ColumnAlreadyExistedException;
import zbl.moonlight.storage.rocks.query.Query;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static zbl.moonlight.storage.rocks.RocksDatabase.COLUMN_FAMILY_ALREADY_EXISTS;

public class TableBatchCreateColumnQuery extends Query<Collection<byte[]>, Void> {
    public TableBatchCreateColumnQuery(Collection<byte[]> queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = queryData.stream()
                .map(ColumnFamilyDescriptor::new)
                .collect(Collectors.toList());

        List<ColumnFamilyHandle> newHandles;

        try {
            newHandles = db.createColumnFamilies(columnFamilyDescriptors);
        } catch (RocksDBException e) {
            Status status = e.getStatus();

            if(COLUMN_FAMILY_ALREADY_EXISTS.equals(status.getState())) {
                throw new ColumnAlreadyExistedException();
            }

            throw e;
        }

        columnFamilyHandles.addAll(newHandles);
    }
}
