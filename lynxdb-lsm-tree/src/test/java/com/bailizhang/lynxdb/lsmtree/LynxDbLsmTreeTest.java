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
import java.util.concurrent.TimeUnit;

class LynxDbLsmTreeTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data";

    private static final String KEY = "key";
    private static final String COLUMN = "column";

    private static final int KEY_COUNT = 10000;
    private static final int COLUMN_COUNT = 40;
    private static final int MEM_TABLE_SIZE = 1000;

    private static final byte[] COLUMN_FAMILY;

    static {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        COLUMN_FAMILY = G.I.toBytes("column_family01");
    }

    private LsmTree lsmTree;

    @BeforeEach
    void setUp() {
        Options options = new Options(MEM_TABLE_SIZE);
        options.wal(false);
        lsmTree = new LynxDbLsmTree(BASE_DIR, options);
    }

    @AfterEach
    void tearDown() {
        G.I.printRecord();
        lsmTree.clear();
    }

    @Test
    void testFunc01() {
        long insertStartTime = System.nanoTime();

        for(int keyCount = KEY_COUNT; keyCount > 0; keyCount --) {
            String key = KEY + keyCount;

            for(int columnCount = 0; columnCount < COLUMN_COUNT; columnCount ++) {
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

        long findStartTime = System.nanoTime();

        for(int keyCount = KEY_COUNT; keyCount > 0; keyCount --) {
            String key = KEY + keyCount;

            for(int columnCount = 0; columnCount < COLUMN_COUNT; columnCount ++) {
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

        long findEndTime = System.nanoTime();

        int total = KEY_COUNT * COLUMN_COUNT;
        long insertTime = findStartTime - insertStartTime;
        insertTime = TimeUnit.MILLISECONDS.convert(insertTime, TimeUnit.NANOSECONDS);

        long findTime = findEndTime - findStartTime;
        findTime = TimeUnit.MILLISECONDS.convert(findTime, TimeUnit.NANOSECONDS);

        System.out.println("Insert Time: " + insertTime);
        System.out.println("Find Time: " + findTime);
        System.out.println("Insert Records: " + (double) total / (double) insertTime * 1000 + " Ops");
        System.out.println("Find Records: " + (double) total / (double) findTime * 1000 + " Ops");
    }
}