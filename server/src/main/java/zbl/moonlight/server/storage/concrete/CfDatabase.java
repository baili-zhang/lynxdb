package zbl.moonlight.server.storage.concrete;

import org.rocksdb.*;
import zbl.moonlight.server.storage.core.AbstractDatabase;
import zbl.moonlight.server.storage.core.ColumnFamilyStorable;
import zbl.moonlight.server.storage.query.CfDeleteQuery;
import zbl.moonlight.server.storage.query.CfGetQuery;
import zbl.moonlight.server.storage.query.CfSetQuery;
import zbl.moonlight.server.storage.query.ResultSet;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class CfDatabase extends AbstractDatabase implements ColumnFamilyStorable {
    private final static String CF_DIR = "cf";

    CfDatabase(String name, String dataDir) {
        super(name, Path.of(dataDir, CF_DIR).toString());
    }

    @Override
    public ResultSet cfGet(CfGetQuery query) {
        final List<ColumnFamilyDescriptor> cfDescriptors = List.of(
                query.isDefault()
                        ? new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY)
                        : new ColumnFamilyDescriptor(query.columnFamily())
        );

        final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

        try(final DBOptions options = new DBOptions().setCreateIfMissing(true);
            final RocksDB db = RocksDB.open(options, path(),
                    cfDescriptors, columnFamilyHandleList)) {
            byte[] value = db.get(columnFamilyHandleList.get(0), query.key());
            return new ResultSet(value);
        } catch (RocksDBException e) {
            System.out.println(e.getStatus().getSubCode().getValue());
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

        try(final DBOptions options = new DBOptions().setCreateIfMissing(true);
            final RocksDB db = RocksDB.open(options, path(),
                    cfDescriptors, columnFamilyHandleList)) {
            db.put(columnFamilyHandleList.get(0), query.key(), query.value());
            return new ResultSet(null);
        } catch (RocksDBException e) {
            System.out.println(e.getStatus().getSubCode().getValue());
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

        try(final DBOptions options = new DBOptions().setCreateIfMissing(true);
            final RocksDB db = RocksDB.open(options, path(),
                    cfDescriptors, columnFamilyHandleList)) {
            db.delete(columnFamilyHandleList.get(0), query.key());
            return new ResultSet(null);
        } catch (RocksDBException e) {
            System.out.println(e.getStatus().getCodeString());
            throw new RuntimeException(e);
        } finally {
            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                columnFamilyHandle.close();
            }
        }
    }
}
