package zbl.moonlight.storage.concrete;

import org.junit.jupiter.api.Test;
import org.rocksdb.*;
import zbl.moonlight.storage.core.*;
import zbl.moonlight.storage.query.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class RocksDatabaseTest {
    private static final byte[] columnFamily = "columnFamily".getBytes(StandardCharsets.UTF_8);
    private static final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);

    @Test
    void doQuery() throws Exception {
        RocksDatabase cfDb = RocksDatabase.open("cf_db_test", System.getProperty("user.dir") + "/data");

        QueryTuple one = new QueryTuple(new ColumnFamily(RocksDB.DEFAULT_COLUMN_FAMILY), new Key(key1), null);
        QueryTuple two = new QueryTuple(new ColumnFamily(columnFamily), new Key(key2), null);

        // 查询 key
        List<QueryTuple> getPairs = new ArrayList<>();
        getPairs.add(one);
        getPairs.add(two);

        Query firstGet = new GetQuery(getPairs);
        ResultSet firstGetResult = cfDb.doQuery(firstGet);
        assert firstGetResult.code() == ResultSet.SUCCESS;

        for(QueryTuple tuple : firstGetResult.result()) {
            assert Arrays.equals(tuple.valueBytes(), null);
        }

        // 设置 key
        List<QueryTuple> setPairs = new ArrayList<>();
        setPairs.add(new QueryTuple(new ColumnFamily(RocksDB.DEFAULT_COLUMN_FAMILY), new Key(key1), new Value(value1)));
        setPairs.add(new QueryTuple(new ColumnFamily(columnFamily), new Key(key2), new Value(value2)));

        Query setQuery = new SetQuery(setPairs);
        ResultSet setResult = cfDb.doQuery(setQuery);
        assert setResult.code() == ResultSet.SUCCESS;

        // 查询 key
        List<QueryTuple> secondGetPairs = new ArrayList<>();
        secondGetPairs.add(one);
        secondGetPairs.add(two);

        Query secondGet = new GetQuery(secondGetPairs);
        ResultSet secondGetResult = cfDb.doQuery(secondGet);
        assert secondGetResult.code() == ResultSet.SUCCESS;

        List<QueryTuple> sft = secondGetResult.result();
        assert Arrays.equals(sft.get(0).valueBytes(), value1);
        assert Arrays.equals(sft.get(1).valueBytes(), value2);

        // 删除 key
        List<QueryTuple> deletePairs = new ArrayList<>();
        deletePairs.add(one);
        deletePairs.add(two);

        Query deleteQuery = new DeleteQuery(deletePairs);
        ResultSet deleteResult = cfDb.doQuery(deleteQuery);
        assert deleteResult.code() == ResultSet.SUCCESS;

        // 再查一遍
        Query lastGet = new GetQuery(getPairs);
        ResultSet lastGetResult = cfDb.doQuery(lastGet);
        assert lastGetResult.code() == ResultSet.SUCCESS;

        for(QueryTuple tuple : lastGetResult.result()) {
            assert Arrays.equals(tuple.valueBytes(), null);
        }

        cfDb.close();
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