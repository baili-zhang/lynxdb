package zbl.moonlight.server.storage.concrete;

import org.rocksdb.*;
import zbl.moonlight.server.storage.core.AbstractDatabase;
import zbl.moonlight.server.storage.core.AbstractNioQuery;
import zbl.moonlight.server.storage.query.cf.CfQuery;
import zbl.moonlight.server.storage.core.ResultSet;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class CfDatabase extends AbstractDatabase {
    public final static String CF_DIR = "cf";

    public CfDatabase(String name, String dataDir) {
        super(name, Path.of(dataDir, CF_DIR).toString());
    }

    @Override
    public synchronized ResultSet doQuery(AbstractNioQuery query) {
        final List<ColumnFamilyDescriptor> cfDescriptors = List.of(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY)
        );

        final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
        ResultSet resultSet = new ResultSet(query.selectionKey());

        try(final DBOptions options = new DBOptions().setCreateIfMissing(true);
            final RocksDB db = RocksDB.open(options, path(),
                    cfDescriptors, columnFamilyHandleList)) {

            query.doQuery(db, resultSet);

        } catch (RocksDBException e) {

            resultSet.setCode(ResultSet.FAILURE);
            resultSet.setMessage(e.getMessage());

        } finally {

            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                columnFamilyHandle.close();
            }

        }

        return resultSet;
    }
}
