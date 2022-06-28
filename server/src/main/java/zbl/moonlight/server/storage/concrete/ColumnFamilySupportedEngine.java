package zbl.moonlight.server.storage.concrete;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.storage.core.ColumnFamilyStorable;
import zbl.moonlight.server.storage.query.CfDeleteQuery;
import zbl.moonlight.server.storage.query.CfGetQuery;
import zbl.moonlight.server.storage.query.CfSetQuery;
import zbl.moonlight.server.storage.query.ResultSet;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO: 哪些资源需要释放？
 */
public class ColumnFamilySupportedEngine implements ColumnFamilyStorable {
    private final String dataDir;

    ColumnFamilySupportedEngine() {
        Configuration config = Configuration.getInstance();
        dataDir = config.dataDir();
    }

    @Override
    public String dataDir() {
        return dataDir;
    }

    @Override
    public ResultSet cfGet(CfGetQuery query) {
        final List<ColumnFamilyDescriptor> cfDescriptors = List.of(
                query.isDefault()
                        ? new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY)
                        : new ColumnFamilyDescriptor(query.columnFamily())
        );

        final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

        try {
            RocksDB db = RocksDB.open(path(query.database()), cfDescriptors, columnFamilyHandleList);
            byte[] value = db.get(columnFamilyHandleList.get(0), query.key());
            return new ResultSet(value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                columnFamilyHandle.close();
            }
        }
    }

    @Override
    public ResultSet cfSet(CfSetQuery query) {
        final List<ColumnFamilyDescriptor> cfDescriptors = List.of(
                query.isDefault()
                        ? new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY)
                        : new ColumnFamilyDescriptor(query.columnFamily())
        );

        final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

        try {
            RocksDB db = RocksDB.open(path(query.database()), cfDescriptors, columnFamilyHandleList);
            db.put(columnFamilyHandleList.get(0), query.key(), query.value());
            return new ResultSet(null);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                columnFamilyHandle.close();
            }
        }
    }

    @Override
    public ResultSet cfDelete(CfDeleteQuery query) {
        final List<ColumnFamilyDescriptor> cfDescriptors = List.of(
                query.isDefault()
                        ? new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY)
                        : new ColumnFamilyDescriptor(query.columnFamily())
        );

        final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

        try {
            RocksDB db = RocksDB.open(path(query.database()), cfDescriptors, columnFamilyHandleList);
            db.delete(columnFamilyHandleList.get(0), query.key());
            return new ResultSet(null);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                columnFamilyHandle.close();
            }
        }
    }
}
