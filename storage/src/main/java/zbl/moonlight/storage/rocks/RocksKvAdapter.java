package zbl.moonlight.storage.rocks;

import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.KvAdapter;
import zbl.moonlight.storage.core.Pair;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.query.kv.*;

import java.nio.file.Path;
import java.util.List;

public class RocksKvAdapter implements KvAdapter {
    private final RocksDatabase db;

    public RocksKvAdapter(String name, String dataDir) {
        try {
            String path = Path.of(dataDir, name).toString();
            db = RocksDatabase.open(path);
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
    public List<Pair<byte[], byte[]>> get(List<byte[]> keys) {
        try {
            ResultSet<List<Pair<byte[], byte[]>>> resultSet = new ResultSet<>();
            db.doQuery(new KvBatchGetQuery(keys, resultSet));
            return resultSet.result();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
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
        try {
            ResultSet<Void> resultSet = new ResultSet<>();
            db.doQuery(new KvBatchSetQuery(kvPairs, resultSet));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
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
        try {
            ResultSet<Void> resultSet = new ResultSet<>();
            db.doQuery(new KvBatchDeleteQuery(keys, resultSet));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        db.close();
    }
}
