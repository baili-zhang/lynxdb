package zbl.moonlight.storage.concrete;

import org.rocksdb.*;
import zbl.moonlight.storage.core.AbstractDatabase;
import zbl.moonlight.storage.core.ColumnFamily;
import zbl.moonlight.storage.core.Queryable;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.query.cf.CfQuery;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class CfDatabase extends AbstractDatabase {
    public final static String CF_DIR = "cf";

    public CfDatabase(String name, String dataDir) {
        super(name, Path.of(dataDir, CF_DIR).toString());
    }

    @Override
    public synchronized ResultSet doQuery(Queryable query) {
        ResultSet resultSet = new ResultSet();

        try (Options options = new Options()) {

            if(query instanceof CfQuery cfQuery) {
                List<byte[]> queryCfs = cfQuery.columnFamilies();
                List<byte[]> cfs = RocksDB.listColumnFamilies(options, path());

                HashSet<ColumnFamily> cfSet = new HashSet<>();
                cfSet.addAll(queryCfs.stream().map(ColumnFamily::new).toList());
                cfSet.addAll(cfs.stream().map(ColumnFamily::new).toList());

                final List<ColumnFamilyDescriptor> cfDescriptors = cfSet.stream()
                        .map(ColumnFamily::value)
                        .map(ColumnFamilyDescriptor::new).toList();

                final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

                try(final DBOptions dbOptions = new DBOptions()
                        .setCreateMissingColumnFamilies(true)
                        .setCreateIfMissing(true);
                    final RocksDB db = RocksDB.open(dbOptions, path(),
                            cfDescriptors, columnFamilyHandleList)) {

                    cfQuery.setColumnFamilyDescriptors(cfDescriptors);
                    cfQuery.setColumnFamilyHandle(columnFamilyHandleList);
                    cfQuery.doQuery(db, resultSet);

                } finally {

                    for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                        columnFamilyHandle.close();
                    }

                }
            } else {

                resultSet.setCode(ResultSet.FAILURE);
                resultSet.setMessage("Query is not an instance of CfQuery");

            }
        } catch (RocksDBException e) {
            resultSet.setCode(ResultSet.FAILURE);
            resultSet.setMessage(e.getMessage());
        }

        return resultSet;
    }
}
