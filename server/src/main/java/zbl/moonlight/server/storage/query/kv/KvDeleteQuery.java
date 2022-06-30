package zbl.moonlight.server.storage.query.kv;

import org.rocksdb.RocksDB;
import zbl.moonlight.server.storage.core.ResultSet;

import java.nio.channels.SelectionKey;

public class KvDeleteQuery extends KvQuery {

    protected KvDeleteQuery(SelectionKey selectionKey, byte[] key) {
        super(selectionKey, key);
    }

    @Override
    public void doQuery(RocksDB db, ResultSet resultSet) {

    }
}
