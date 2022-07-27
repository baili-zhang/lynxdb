package zbl.moonlight.storage.rocks;

import org.rocksdb.*;
import zbl.moonlight.storage.core.Database;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.query.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksDatabase extends Database {
    private final Options options;

    private final DBOptions dbOptions;

    private final RocksDB rocksDB;

    private final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    private final ColumnFamilyHandle defaultHandle;

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

        ColumnFamilyHandle defaultHandle = null;
        for(ColumnFamilyHandle handle : columnFamilyHandles) {
            if(Arrays.equals(handle.getName(), RocksDB.DEFAULT_COLUMN_FAMILY)) {
                defaultHandle = handle;
            }
        }

        if(defaultHandle == null) {
            throw new RuntimeException("Default column family handle is null.");
        }

        this.defaultHandle = defaultHandle;
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
    public synchronized ResultSet<?> doQuery(Query<?, ?> query) throws RocksDBException {
        query.setDefaultHandle(defaultHandle);
        query.setColumnFamilyHandles(columnFamilyHandles);
        query.doQuery(rocksDB);
        return query.resultSet();
    }
}
