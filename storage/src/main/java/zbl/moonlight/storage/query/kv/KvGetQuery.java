package zbl.moonlight.storage.query.kv;

import org.rocksdb.RocksDB;
import zbl.moonlight.storage.core.ResultSet;

import java.nio.channels.SelectionKey;

public class KvGetQuery extends KvQuery {

    protected KvGetQuery(SelectionKey selectionKey, byte[] key) {
        super(selectionKey, key);
    }

    @Override
    public void doQuery(RocksDB db, ResultSet resultSet) {

    }
}
