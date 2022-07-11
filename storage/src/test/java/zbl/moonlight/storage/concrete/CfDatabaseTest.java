package zbl.moonlight.storage.concrete;

import org.junit.jupiter.api.Test;
import org.rocksdb.*;
import zbl.moonlight.storage.core.*;
import zbl.moonlight.storage.query.cf.CfDeleteQuery;
import zbl.moonlight.storage.query.cf.CfGetQuery;
import zbl.moonlight.storage.query.cf.CfQuery;
import zbl.moonlight.storage.query.cf.CfSetQuery;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class CfDatabaseTest {
    private static final byte[] columnFamily = "columnFamily".getBytes(StandardCharsets.UTF_8);
    private static final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);

    @Test
    void doQuery() {
        CfDatabase cfDb = new CfDatabase("cf_db_test", System.getProperty("user.dir") + "/data");

        ColumnFamilyTuple one = new ColumnFamilyTuple(new ColumnFamily(RocksDB.DEFAULT_COLUMN_FAMILY), new Key(key1), null);
        ColumnFamilyTuple two = new ColumnFamilyTuple(new ColumnFamily(columnFamily), new Key(key2), null);

        // 查询 key
        List<ColumnFamilyTuple> getPairs = new ArrayList<>();
        getPairs.add(one);
        getPairs.add(two);

        CfQuery firstGet = new CfGetQuery(getPairs);
        ResultSet firstGetResult = cfDb.doQuery(firstGet);
        assert firstGetResult.code() == ResultSet.SUCCESS;

        for(ColumnFamilyTuple tuple : firstGetResult.result()) {
            assert Arrays.equals(tuple.valueBytes(), null);
        }

        // 设置 key
        List<ColumnFamilyTuple> setPairs = new ArrayList<>();
        setPairs.add(new ColumnFamilyTuple(new ColumnFamily(RocksDB.DEFAULT_COLUMN_FAMILY), new Key(key1), new Value(value1)));
        setPairs.add(new ColumnFamilyTuple(new ColumnFamily(columnFamily), new Key(key2), new Value(value2)));

        CfQuery setQuery = new CfSetQuery(setPairs);
        ResultSet setResult = cfDb.doQuery(setQuery);
        assert setResult.code() == ResultSet.SUCCESS;

        // 查询 key
        List<ColumnFamilyTuple> secondGetPairs = new ArrayList<>();
        secondGetPairs.add(one);
        secondGetPairs.add(two);

        CfQuery secondGet = new CfGetQuery(secondGetPairs);
        ResultSet secondGetResult = cfDb.doQuery(secondGet);
        assert secondGetResult.code() == ResultSet.SUCCESS;

        List<ColumnFamilyTuple> sft = secondGetResult.result();
        assert Arrays.equals(sft.get(0).valueBytes(), value1);
        assert Arrays.equals(sft.get(1).valueBytes(), value2);

        // 删除 key
        List<ColumnFamilyTuple> deletePairs = new ArrayList<>();
        deletePairs.add(one);
        deletePairs.add(two);

        CfQuery deleteQuery = new CfDeleteQuery(deletePairs);
        ResultSet deleteResult = cfDb.doQuery(deleteQuery);
        assert deleteResult.code() == ResultSet.SUCCESS;

        // 再查一遍
        CfQuery lastGet = new CfGetQuery(getPairs);
        ResultSet lastGetResult = cfDb.doQuery(lastGet);
        assert lastGetResult.code() == ResultSet.SUCCESS;

        for(ColumnFamilyTuple tuple : lastGetResult.result()) {
            assert Arrays.equals(tuple.valueBytes(), null);
        }
    }

    @Test
    void test() {
        String path = System.getProperty("user.dir") + "/data/cf/cf_db_test";

        try (final Options options = new Options()) {
            List<byte[]> cfs = RocksDB.listColumnFamilies(options, path);
            final List<ColumnFamilyDescriptor> cfDescriptors = cfs.stream().map(ColumnFamilyDescriptor::new).toList();

            final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

            try(final DBOptions dbOptions = new DBOptions()
                    .setCreateMissingColumnFamilies(true)
                    .setCreateIfMissing(true);
                final RocksDB db = RocksDB.open(dbOptions, path,
                        cfDescriptors, columnFamilyHandleList)) {
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