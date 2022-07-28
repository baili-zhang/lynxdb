package zbl.moonlight.storage.rocks;

import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.KvAdapter;
import zbl.moonlight.storage.core.Pair;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.query.kv.KvSingleDeleteQuery;
import zbl.moonlight.storage.rocks.query.kv.KvSingleGetQuery;
import zbl.moonlight.storage.rocks.query.kv.KvSingleSetQuery;

import java.util.List;

public class RocksKvAdapter implements KvAdapter {
    private final RocksDatabase db;

    public RocksKvAdapter(String name, String dataDir) {
        try {
            db = RocksDatabase.open(name, dataDir);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            ResultSet<byte[]> resultSet = new ResultSet<>();
            db.doQuery(new KvSingleGetQuery(key, resultSet));
            return resultSet.result();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<byte[]> get(List<byte[]> keys) {
        return null;
    }

    @Override
    public void set(Pair<byte[], byte[]> kvPair) {
        try {
            ResultSet<Void> resultSet = new ResultSet<>();
            db.doQuery(new KvSingleSetQuery(kvPair, resultSet));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(List<Pair<byte[], byte[]>> kvPairs) {

    }

    @Override
    public void delete(byte[] key) {
        try {
            ResultSet<Void> resultSet = new ResultSet<>();
            db.doQuery(new KvSingleDeleteQuery(key, resultSet));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(List<byte[]> keys) {

    }
}
