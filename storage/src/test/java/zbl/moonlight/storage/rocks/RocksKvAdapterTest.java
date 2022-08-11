package zbl.moonlight.storage.rocks;

import org.junit.jupiter.api.*;
import zbl.moonlight.storage.core.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@TestMethodOrder(MethodOrderer.MethodName.class)
class RocksKvAdapterTest {
    private static final String DB_NAME = "test_db";
    private static final String DB_DIR = System.getProperty("user.dir") + "/data/kv/";

    private RocksKvAdapter db;

    interface Constant {
        String KEY_STR = "key";
        String VALUE_STR = "value";

        byte[] KEY = "key".getBytes();
        byte[] VALUE = "value".getBytes();

        List<byte[]> KEY_LIST = new ArrayList<>();
        List<byte[]> VALUE_LIST = new ArrayList<>();
    }

    @BeforeEach
    void setUp() {
        db = new RocksKvAdapter(DB_NAME, DB_DIR);

        for(int i = 0; i < 100; i ++) {
            Constant.KEY_LIST.add((Constant.KEY_STR + i).getBytes());
            Constant.VALUE_LIST.add((Constant.VALUE_STR + i).getBytes());
        }
    }

    @AfterEach
    void tearDown() {
        try {
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void test_001_Get() {
        byte[] value =  db.get(Constant.KEY);
        assert value == null;
    }


    @Test
    void test_002_Set() {
        db.set(new Pair<>(Constant.KEY, Constant.VALUE));
        byte[] value = db.get(Constant.KEY);

        assert Arrays.equals(value, Constant.VALUE);
    }

    @Test
    void test_003_Delete() {
        db.delete(Constant.KEY);

        test_001_Get();
    }

    @Test
    void test_004_Func() {
        test_001_Get();
        test_002_Set();
        test_003_Delete();
    }

    @Test
    void test_005_Batch_Get() {
        List<Pair<byte[], byte[]>> kvPairs = db.get(Constant.KEY_LIST);

        for(Pair<byte[], byte[]> pair : kvPairs) {
            assert pair.right() == null;
        }
    }

    @Test
    void test_006_Batch_Set() {
        List<Pair<byte[], byte[]>> kvPairs = new ArrayList<>();

        for(int i = 0; i < 100; i ++) {
            byte[] key = Constant.KEY_LIST.get(i);
            byte[] value = Constant.VALUE_LIST.get(i);
            kvPairs.add(new Pair<>(key, value));
        }

        db.set(kvPairs);

        List<Pair<byte[], byte[]>> readKvPairs = db.get(Constant.KEY_LIST);

        for (int i = 0; i < 100; i++) {
            assert Arrays.equals(kvPairs.get(i).right(), readKvPairs.get(i).right());
        }
    }

    @Test
    void test_007_Batch_Delete() {
        db.delete(Constant.KEY_LIST);

        test_005_Batch_Get();
    }

    @Test
    void test_008_Batch_Func() {
        test_005_Batch_Get();
        test_006_Batch_Set();
        test_007_Batch_Delete();
    }
}