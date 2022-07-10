package zbl.moonlight.storage.concrete;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.AbstractDatabase;
import zbl.moonlight.storage.core.AbstractNioQuery;
import zbl.moonlight.storage.core.Queryable;
import zbl.moonlight.storage.core.ResultSet;

import java.nio.file.Path;

public class KvDatabase extends AbstractDatabase {
    public final static String KV_DIR = "kv";

    public KvDatabase(String name, String dataDir) {
        super(name, Path.of(dataDir, KV_DIR).toString());
    }

    @Override
    public synchronized ResultSet doQuery(Queryable query) {
        ResultSet resultSet = new ResultSet();

        try(final RocksDB db = RocksDB.open(path())) {
            query.doQuery(db, resultSet);
        } catch (RocksDBException e) {
            resultSet.setCode(ResultSet.FAILURE);
            resultSet.setMessage(e.getMessage());
        }

        return resultSet;
    }
}
