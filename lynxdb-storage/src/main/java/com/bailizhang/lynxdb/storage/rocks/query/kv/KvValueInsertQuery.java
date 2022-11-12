package com.bailizhang.lynxdb.storage.rocks.query.kv;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.WrappedBytes;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.storage.core.Pair;
import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;

public class KvValueInsertQuery extends Query<Pair<byte[], List<byte[]>>, Void> {
    public KvValueInsertQuery(Pair<byte[], List<byte[]>> queryData,
                                 ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        byte[] key = queryData.left();
        byte[] value = db.get(key);

        if(value == null) {
            value = BufferUtils.EMPTY_BYTES;
        }

        HashSet<WrappedBytes> set = new HashSet<>();
        ByteBuffer buffer = ByteBuffer.wrap(value);

        while(BufferUtils.isNotOver(buffer)) {
            byte[] bytes = BufferUtils.getBytes(buffer);
            WrappedBytes item = new WrappedBytes(bytes);
            set.add(item);
        }

        List<byte[]> appendValues = queryData.right();
        appendValues.forEach(bytes -> set.add(new WrappedBytes(bytes)));

        BytesList bytesList = new BytesList(false);
        set.forEach(item -> bytesList.appendVarBytes(item.bytes()));

        db.put(key, bytesList.toBytes());
    }
}
