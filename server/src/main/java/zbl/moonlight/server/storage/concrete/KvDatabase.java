package zbl.moonlight.server.storage.concrete;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.server.storage.core.AbstractDatabase;
import zbl.moonlight.server.storage.core.KeyValueStorable;
import zbl.moonlight.server.storage.query.KvDeleteQuery;
import zbl.moonlight.server.storage.query.KvGetQuery;
import zbl.moonlight.server.storage.query.KvSetQuery;
import zbl.moonlight.server.storage.query.ResultSet;

import java.nio.file.Path;

public class KvDatabase extends AbstractDatabase implements KeyValueStorable {
    private final static String KV_DIR = "kv";

    KvDatabase(String name, String dataDir) {
        super(name, Path.of(dataDir, KV_DIR).toString());
    }

    @Override
    public ResultSet kvGet(KvGetQuery query) {
        try(final RocksDB db = RocksDB.open(path())) {
            byte[] value = db.get(query.key());
            return new ResultSet(value);
        } catch (RocksDBException e) {
            // TODO: 做好错误处理
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResultSet kvSet(KvSetQuery query) {
        try(final RocksDB db = RocksDB.open(path())) {
            db.put(query.key(), query.value());
            return new ResultSet(null);
        } catch (RocksDBException e) {
            // TODO: 做好错误处理
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResultSet kvDelete(KvDeleteQuery query) {
        try(final RocksDB db = RocksDB.open(path())) {
            db.delete(query.key());
            return new ResultSet(null);
        } catch (RocksDBException e) {
            // TODO: 做好错误处理
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
