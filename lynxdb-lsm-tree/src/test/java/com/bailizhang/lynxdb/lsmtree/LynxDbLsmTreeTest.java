package com.bailizhang.lynxdb.lsmtree;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.lsmtree.common.LsmTree;
import com.bailizhang.lynxdb.lsmtree.common.Options;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

class LynxDbLsmTreeTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data";

    private static final String KEY = "key";
    private static final String COLUMN = "column";

    private static final byte[] COLUMN_FAMILY;

    static {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        COLUMN_FAMILY = G.I.toBytes("column_family01");
    }

    private LsmTree lsmTree;

    @BeforeEach
    void setUp() {
        Options options = new Options(4000);
        lsmTree = new LynxDbLsmTree(BASE_DIR, options);
    }

    @AfterEach
    void tearDown() {
        // lsmTree.clear();
    }

    @Test
    void testFunc01() {
        for(int keyCount = 0; keyCount < 11000; keyCount ++) {
            for(int columnCount = 0; columnCount < 400; columnCount ++) {
                String key = KEY + keyCount;
                String column = COLUMN + columnCount;
                String value = key + column;

                lsmTree.insert(
                        G.I.toBytes(key),
                        COLUMN_FAMILY,
                        G.I.toBytes(column),
                        G.I.toBytes(value)
                );
            }
        }

        for(int keyCount = 0; keyCount < 200; keyCount ++) {
            for(int columnCount = 0; columnCount < 10; columnCount ++) {
                String key = KEY + keyCount;
                String column = COLUMN + columnCount;
                byte[] value = G.I.toBytes(key + column);

                byte[] findValue = lsmTree.find(
                        G.I.toBytes(key),
                        COLUMN_FAMILY,
                        G.I.toBytes(column)
                );

                assert Arrays.equals(value, findValue);
            }
        }
    }
}