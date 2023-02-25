package com.bailizhang.test.lynxdb.lsmtree;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.lsmtree.Table;
import com.bailizhang.lynxdb.lsmtree.LynxDbLsmTree;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

class LynxDbLsmTreeTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data";

    private static final String KEY = "dbKey";
    private static final String COLUMN = "column";

    private static final int KEY_COUNT = 1000;
    private static final int COLUMN_COUNT = 90;
    private static final int MEM_TABLE_SIZE = 400;

    private static final String COLUMN_FAMILY = "column_family01";

    static {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
    }

    private Table lsmTree;

    @BeforeEach
    void setUp() {
        LsmTreeOptions options = new LsmTreeOptions(BASE_DIR, MEM_TABLE_SIZE);
        lsmTree = new LynxDbLsmTree(options);
    }

    @AfterEach
    void tearDown() {
        // lsmTree.clear();
    }

    @Test
    void insert() {
        for(int columnCount = 0; columnCount < COLUMN_COUNT; columnCount ++) {
            String column = COLUMN + columnCount;

            for(int keyCount = KEY_COUNT; keyCount > 0; keyCount --) {
                String key = KEY + keyCount;
                String value = key + column;

                lsmTree.insert(
                        G.I.toBytes(key),
                        COLUMN_FAMILY,
                        column,
                        G.I.toBytes(value)
                );
            }
        }

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testFunc01() {
        // insert();

        for(int keyCount = KEY_COUNT; keyCount > 0; keyCount --) {
            String key = KEY + keyCount;

            for(int columnCount = 0; columnCount < COLUMN_COUNT; columnCount ++) {
                String column = COLUMN + columnCount;
                byte[] value = G.I.toBytes(key + column);

                byte[] findValue = lsmTree.find(
                        G.I.toBytes(key),
                        COLUMN_FAMILY,
                        column
                );

                assert Arrays.equals(value, findValue);
            }
        }
    }

    @Test
    void testFunc02() {
        insert();

        String key = KEY + 500;

        HashMap<String, byte[]> multiColumns = lsmTree.find(
                G.I.toBytes(key),
                COLUMN_FAMILY
        );

        assert multiColumns.size() == COLUMN_COUNT;
    }

    @Test
    void testFunc03() {
        insert();

        String key = KEY + 500;
        String column = COLUMN + 1;

        lsmTree.delete(
                G.I.toBytes(key),
                COLUMN_FAMILY,
                column
        );

        HashMap<String, byte[]> dbValues = lsmTree.find(
                G.I.toBytes(key),
                COLUMN_FAMILY
        );

        assert dbValues.size() == COLUMN_COUNT - 1;
    }

    @Test
    void testFunc04() {
        byte[] key = G.I.toBytes("Hallo");
        String column = "World";
        byte[] value = G.I.toBytes("LynxDb");

        lsmTree.insert(
                key,
                COLUMN_FAMILY,
                column,
                value
        );

        insert();

        byte[] dbValue = lsmTree.find(
                key,
                COLUMN_FAMILY,
                column
        );

        assert Arrays.equals(dbValue, value);

        lsmTree.delete(
                key,
                COLUMN_FAMILY,
                column
        );

        insert();

        dbValue = lsmTree.find(
                key,
                COLUMN_FAMILY,
                column
        );

        assert Arrays.equals(dbValue, null);
    }
}