package com.bailizhang.lynxdb.storage.rocks.query.table;

import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.core.exception.ColumnAlreadyExistedException;
import com.bailizhang.lynxdb.storage.rocks.query.Query;
import org.rocksdb.*;

import static com.bailizhang.lynxdb.storage.rocks.RocksDatabase.COLUMN_FAMILY_ALREADY_EXISTS;

public class TableSingleCreateColumnQuery extends Query<byte[], Void> {
    public TableSingleCreateColumnQuery(byte[] queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        ColumnFamilyHandle newHandle;

        try {
            newHandle = db.createColumnFamily(new ColumnFamilyDescriptor(queryData));
        } catch (RocksDBException e) {
            Status status = e.getStatus();

            if(COLUMN_FAMILY_ALREADY_EXISTS.equals(status.getState())) {
                throw new ColumnAlreadyExistedException();
            }

            throw e;
        }

        columnFamilyHandles.add(newHandle);
    }
}
