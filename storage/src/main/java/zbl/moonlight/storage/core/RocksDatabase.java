package zbl.moonlight.storage.core;

import org.rocksdb.*;
import zbl.moonlight.storage.query.Query;

import java.util.ArrayList;
import java.util.List;

public class RocksDatabase extends Database {
    private final Options options;

    private final DBOptions dbOptions;

    private final RocksDB rocksDB;

    private final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    private RocksDatabase(String name, String dataDir) throws RocksDBException {
        super(name, dataDir);
        options = new Options();
        dbOptions = new DBOptions().setCreateIfMissing(true);

        List<byte[]> cfs = RocksDB.listColumnFamilies(options, path());
        if(cfs.isEmpty()) {
            cfs.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        }

        final List<ColumnFamilyDescriptor> cfDescriptors = cfs.stream()
                .map(ColumnFamilyDescriptor::new).toList();
        rocksDB = RocksDB.open(dbOptions, path(), cfDescriptors, columnFamilyHandles);
    }

    @Override
    public void close() throws Exception {
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            columnFamilyHandle.close();
        }

        rocksDB.close();
        dbOptions.close();
        options.close();
    }

    public static RocksDatabase open(String name, String dataDir) throws RocksDBException {
        return new RocksDatabase(name, dataDir);
    }

    @Override
    public synchronized ResultSet doQuery(Query query) throws RocksDBException {
        ResultSet resultSet = new ResultSet();
        query.setColumnFamilyHandles(columnFamilyHandles);
        query.doQuery(rocksDB, resultSet);
        return resultSet;
    }
}
