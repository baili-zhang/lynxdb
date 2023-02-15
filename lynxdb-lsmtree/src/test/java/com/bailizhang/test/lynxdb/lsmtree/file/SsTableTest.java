package com.bailizhang.test.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.lsmtree.common.DbIndex;
import com.bailizhang.lynxdb.lsmtree.common.DbKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class SsTableTest {

    private static final String BASE_DIR = System.getProperty("user.dir") + "/data";

    private static final String KEY = "dbKey";
    private static final String COLUMN = "column";

    private static final byte[] COLUMN_FAMILY;

    static {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        COLUMN_FAMILY = G.I.toBytes("column_family01");
    }

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void create() {
        List<DbIndex> dbIndexList = new ArrayList<>();

        for(int keyCount = 0; keyCount < 100; keyCount ++) {
            for(int columnCount = 0; columnCount < 40; columnCount ++) {
                String key = KEY + keyCount;
                String column = COLUMN + columnCount;

                DbKey dbKey = new DbKey(
                        G.I.toBytes(key),
                        G.I.toBytes(column),
                        DbKey.EXISTED
                );

                dbIndexList.add(new DbIndex(dbKey, 10));
            }
        }
    }
}