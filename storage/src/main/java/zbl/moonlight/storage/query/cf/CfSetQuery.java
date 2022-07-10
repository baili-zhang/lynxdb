package zbl.moonlight.storage.query.cf;

import org.rocksdb.RocksDB;
import zbl.moonlight.storage.core.ResultSet;

import java.nio.channels.SelectionKey;

public class CfSetQuery extends CfQueryWithValue {
    protected CfSetQuery(SelectionKey selectionKey, byte[] columnFamily, byte[] key, byte[] value) {
        super(selectionKey, columnFamily, key, value);
    }

    @Override
    public void doQuery(RocksDB db, ResultSet resultSet) {

    }
}
