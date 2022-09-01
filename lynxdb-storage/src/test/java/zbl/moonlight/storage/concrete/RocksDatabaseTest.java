package zbl.moonlight.storage.concrete;

import org.junit.jupiter.api.Test;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class RocksDatabaseTest {
    private static final byte[] columnFamily = "columnFamily".getBytes(StandardCharsets.UTF_8);
    private static final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);

    @Test
    void doQuery() throws Exception {

    }

    @Test
    void test() {
        String path = System.getProperty("user.dir") + "/data/cf/cf_db_test";

        try (final Options options = new Options()) {
            List<byte[]> cfs = RocksDB.listColumnFamilies(options, path);
            List<ColumnFamilyDescriptor> cfDescriptors = cfs.stream().map(ColumnFamilyDescriptor::new).toList();

            final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

            try(final DBOptions dbOptions = new DBOptions();
                final RocksDB db = RocksDB.open(dbOptions, path,
                        cfDescriptors, columnFamilyHandleList)) {
                cfs = RocksDB.listColumnFamilies(options, path);
                cfs.forEach(bytes -> System.out.println(new String(bytes)));
                System.out.println(new String(columnFamilyHandleList.get(4).getName()));
                db.put(columnFamilyHandleList.get(0), key1, value1);
                db.delete(columnFamilyHandleList.get(0), key1);

            } catch (RocksDBException e) {
                e.printStackTrace();

            } finally {

                for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                    columnFamilyHandle.close();
                }

            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

    }
}