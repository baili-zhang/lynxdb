package com.bailizhang.lynxdb.storage.rocks.query.kv;

import com.bailizhang.lynxdb.storage.core.Pair;
import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.List;

public class KvBatchSetQuery extends Query<List<Pair<byte[], byte[]>>, Void> {
    public KvBatchSetQuery(List<Pair<byte[], byte[]>> queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        try (final WriteBatch writeBatch = new WriteBatch();
             final WriteOptions writeOptions = new WriteOptions()) {

            for(Pair<byte[], byte[]> pair : queryData) {
                writeBatch.put(pair.left(), pair.right());
            }

            db.write(writeOptions, writeBatch);
        }
    }
}
