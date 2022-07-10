package zbl.moonlight.storage.concrete;

import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;
import zbl.moonlight.storage.core.ColumnFamily;
import zbl.moonlight.storage.core.ColumnFamilyTuple;
import zbl.moonlight.storage.core.Key;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.query.cf.CfGetQuery;
import zbl.moonlight.storage.query.cf.CfQuery;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class CfDatabaseTest {
    private static final byte[] columnFamily = "columnFamily".getBytes(StandardCharsets.UTF_8);
    private static final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value = "value".getBytes(StandardCharsets.UTF_8);

    @Test
    void doQuery() {
        CfDatabase cfDb = new CfDatabase("cf_db_test", System.getProperty("user.dir") + "/data");

        List<ColumnFamilyTuple> pairs = new ArrayList<>();
        pairs.add(new ColumnFamilyTuple(new ColumnFamily(RocksDB.DEFAULT_COLUMN_FAMILY), new Key(key), null));

        // 直接查询
        CfQuery firstGet = new CfGetQuery(pairs);
        ResultSet firstGetResult = cfDb.doQuery(firstGet);
        assert firstGetResult.code() == ResultSet.SUCCESS;

        for(ColumnFamilyTuple tuple : firstGetResult.result()) {
            assert Arrays.equals(tuple.valueBytes(), null);
        }

        // 设置默认列族的 key

        // 查询默认列族的 key

        // 删除默认列族的 key

        // 再查一遍
    }
}