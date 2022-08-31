package zbl.moonlight.storage.rocks;

import com.bailizhang.lynxdb.storage.core.*;
import com.bailizhang.lynxdb.storage.rocks.RocksTableAdapter;
import org.junit.jupiter.api.*;

import java.nio.file.Path;
import java.util.*;

@TestMethodOrder(MethodOrderer.MethodName.class)
class RocksTableAdapterTest {
    private static final String DB_NAME = "test_db";
    private static final String DB_DIR = System.getProperty("user.dir") + "/data/table/";

    private RocksTableAdapter db;

    interface Constant {
        String KEY_STR = "key";
        String VALUE_STR = "value";
        String COLUMN_STR = "column";

        String VALUE_TEMP = "%s_%s";

        byte[] KEY = KEY_STR.getBytes();
        byte[] VALUE = VALUE_STR.getBytes();
        byte[] COLUMN = COLUMN_STR.getBytes();

        List<byte[]> KEYS = new ArrayList<>();
        HashSet<Column> COLUMNS = new HashSet<>();
        List<byte[]> VALUES = new ArrayList<>();
        MultiTableRows MULTI_ROWS = new MultiTableRows();
    }

    @BeforeEach
    void setUp() {
        Path path = Path.of(DB_DIR, DB_NAME);
        db = new RocksTableAdapter(path.toString());

        for(int i = 0; i < 10; i ++) {
            byte[] bytes = (Constant.COLUMN_STR + i).getBytes();
            Column column = new Column(bytes);
            Constant.COLUMNS.add(column);

            bytes = (Constant.VALUE_STR + i).getBytes();
            Constant.VALUES.add(bytes);

            bytes = (Constant.KEY_STR + i).getBytes();
            Constant.KEYS.add(bytes);
        }

        for(byte[] key : Constant.KEYS) {
            Map<Column, byte[]> row = new HashMap<>();
            for(Column column : Constant.COLUMNS) {
                String value = String.format(Constant.VALUE_TEMP, new String(key), new String(column.value()));
                row.put(column, value.getBytes());
            }
            Constant.MULTI_ROWS.put(new Key(key), row);
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
    void test_001_Get_Null() {
        SingleTableKey key = new SingleTableKey(Constant.KEY, Constant.COLUMNS);
        SingleTableRow row = db.get(key);

        for(Column column : Constant.COLUMNS) {
            byte[] value = row.get(column);
            assert value == null;
        }
    }

    void setRow() {
        SingleTableRow row = new SingleTableRow(Constant.KEY);

        int i = 0;
        for(Column column : Constant.COLUMNS) {
            row.put(column, Constant.VALUES.get(i ++));
        }

        db.set(row);
    }

    @Test
    void test_002_Set_Failed() {
        setRow();
        test_001_Get_Null();
    }

    void createColumn() {
        db.createColumns(Constant.COLUMNS.stream().map(Column::value).toList());
    }

    @Test
    void test_003_Set_Successfully() {
        createColumn();

        setRow();

        SingleTableKey key = new SingleTableKey(Constant.KEY, Constant.COLUMNS);
        SingleTableRow row = db.get(key);

        int i = 0;
        for(Column column : Constant.COLUMNS) {
            byte[] value = row.get(column);
            assert Arrays.equals(value, Constant.VALUES.get(i ++));
        }
    }

    @Test
    void test_004_Delete() {
        db.delete(Constant.KEY);

        test_001_Get_Null();

        dropColumn();
    }

    void dropColumn() {
        db.dropColumns(Constant.COLUMNS);
    }

    @Test
    void test_005_Batch_Get_Null() {
        db.set(Constant.MULTI_ROWS);

        MultiTableKeys keys = new MultiTableKeys(Constant.KEYS, Constant.COLUMNS);
        MultiTableRows rows = db.get(keys);

        for(byte[] key : Constant.KEYS) {
            Map<Column, byte[]> row = rows.get(new Key(key));
            for(Column column : Constant.COLUMNS) {
                byte[] value = row.get(column);
                assert value == null;
            }
        }
    }

    @Test
    void test_006_Batch_Get_Successfully() {
        createColumn();

        db.set(Constant.MULTI_ROWS);

        MultiTableKeys keys = new MultiTableKeys(Constant.KEYS, Constant.COLUMNS);
        MultiTableRows rows = db.get(keys);

        for(byte[] bytes : Constant.KEYS) {
            Key key = new Key(bytes);
            Map<Column, byte[]> resultRow = rows.get(key);
            Map<Column, byte[]> row = Constant.MULTI_ROWS.get(key);
            for(Column column : Constant.COLUMNS) {
                byte[] resultValue = resultRow.get(column);
                byte[] value = row.get(column);
                assert Arrays.equals(resultValue, value);
            }
        }
    }

    @Test
    void test_007_Batch_Delete() {
        db.delete(Constant.KEYS);

        MultiTableKeys keys = new MultiTableKeys(Constant.KEYS, Constant.COLUMNS);
        MultiTableRows rows = db.get(keys);

        for(byte[] key : Constant.KEYS) {
            Map<Column, byte[]> row = rows.get(new Key(key));
            for(Column column : Constant.COLUMNS) {
                byte[] value = row.get(column);
                assert value == null;
            }
        }

        db.dropColumns(Constant.COLUMNS);
    }
}