package zbl.moonlight.storage.concrete;

import org.rocksdb.*;
import zbl.moonlight.storage.core.AbstractDatabase;
import zbl.moonlight.storage.core.Queryable;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.query.cf.CfQuery;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CfDatabase extends AbstractDatabase {
    public final static String CF_DIR = "cf";

    public CfDatabase(String name, String dataDir) {
        super(name, Path.of(dataDir, CF_DIR).toString());
    }

    @Override
    public synchronized ResultSet doQuery(Queryable query) {
        ResultSet resultSet = new ResultSet();

        if(query instanceof CfQuery cfQuery) {
            final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
            cfDescriptors.addAll(cfQuery.columnFamilies().stream()
                    .filter(cf -> !Arrays.equals(cf, RocksDB.DEFAULT_COLUMN_FAMILY))
                    .map(ColumnFamilyDescriptor::new).toList());

            final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

            try(final DBOptions options = new DBOptions()
                    .setCreateMissingColumnFamilies(true)
                    .setCreateIfMissing(true);
                final RocksDB db = RocksDB.open(options, path(),
                        cfDescriptors, columnFamilyHandleList)) {

                cfQuery.setColumnFamilyDescriptors(cfDescriptors);
                cfQuery.setColumnFamilyHandle(columnFamilyHandleList);
                cfQuery.doQuery(db, resultSet);

            } catch (RocksDBException e) {

                resultSet.setCode(ResultSet.FAILURE);
                resultSet.setMessage(e.getMessage());

            } finally {

                for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                    columnFamilyHandle.close();
                }

            }
        } else {

            resultSet.setCode(ResultSet.FAILURE);
            resultSet.setMessage("Query is not an instance of CfQuery");

        }

        return resultSet;
    }
}
