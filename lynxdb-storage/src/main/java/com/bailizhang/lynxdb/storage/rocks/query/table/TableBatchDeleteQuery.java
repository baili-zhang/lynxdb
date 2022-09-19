package com.bailizhang.lynxdb.storage.rocks.query.table;

import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;
import org.rocksdb.*;

import java.util.List;

public class TableBatchDeleteQuery extends Query<List<byte[]>, Void> {
    public TableBatchDeleteQuery(List<byte[]> queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        try(final WriteBatch writeBatch = new WriteBatch();
            final WriteOptions writeOptions = new WriteOptions()) {

            for(byte[] key : queryData) {
                for(ColumnFamilyHandle handle : columnFamilyHandles) {
                    writeBatch.delete(handle, key);
                }
            }

            db.write(writeOptions, writeBatch);
        }
    }
}
