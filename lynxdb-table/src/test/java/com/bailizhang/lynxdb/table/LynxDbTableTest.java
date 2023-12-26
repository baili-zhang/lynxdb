/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.table;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.table.config.LsmTreeOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.locks.LockSupport;

class LynxDbTableTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data/table_test";

    private static final String KEY = "dbKey";
    private static final String COLUMN = "column";

    private static final int KEY_COUNT = 800;
    private static final int COLUMN_COUNT = 10;
    private static final int MEM_TABLE_SIZE = 20;

    private static final String COLUMN_FAMILY = "column_family01";

    static {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
    }

    private Table lsmTree;

    @BeforeEach
    void setUp() {
        LsmTreeOptions options = new LsmTreeOptions(BASE_DIR, MEM_TABLE_SIZE);
        options.wal(true);
        lsmTree = new LynxDbTable(options);
    }

    @AfterEach
    void tearDown() {
        lsmTree.clear();
    }

    void insert(long timeout) {
        for(int columnCount = 0; columnCount < COLUMN_COUNT; columnCount ++) {
            String column = COLUMN + columnCount;

            for(int keyCount = KEY_COUNT; keyCount > 0; keyCount --) {
                String key = KEY + keyCount;
                String value = key + column;

                lsmTree.insert(
                        G.I.toBytes(key),
                        COLUMN_FAMILY,
                        column,
                        G.I.toBytes(value),
                        timeout
                );
            }
        }
    }

    @Test
    void insert() {
        insert(-1);
    }

    @Test
    void testFunc01() {
        insert();

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

        HashMap<String, byte[]> multiColumns = lsmTree.findMultiColumns(
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

        HashMap<String, byte[]> multiColumns = lsmTree.findMultiColumns(
                G.I.toBytes(key),
                COLUMN_FAMILY
        );

        assert multiColumns.size() == COLUMN_COUNT - 1;
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
                value,
                -1
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

    @Test
    void testFunc05() {
        insert();

        byte[] key = G.I.toBytes(KEY + 5001);
        String column = COLUMN + 1;

        assert !lsmTree.existKey(key, COLUMN_FAMILY, column);

        byte[] value = G.I.toBytes("value");
        lsmTree.insert(key, COLUMN_FAMILY, column, value, -1);

        assert lsmTree.existKey(key, COLUMN_FAMILY, column);

        lsmTree.delete(key, COLUMN_FAMILY, column);
        assert !lsmTree.existKey(key, COLUMN_FAMILY, column);
    }

    @Test
    void testFunc06() {
        insert();

        String column = COLUMN + 1;
        byte[] beginKey = G.I.toBytes(KEY + 5001);

        var multiKeys = lsmTree.rangeNext(
                COLUMN_FAMILY,
                column,
                beginKey,
                10
        );

        assert multiKeys.size() == 10;
    }

    @Test
    void testFunc07() {
        insert();

        String column = COLUMN + 1;
        byte[] endKey = G.I.toBytes(KEY + 5001);

        var multiKeys = lsmTree.rangeBefore(
                COLUMN_FAMILY,
                column,
                endKey,
                10
        );

        assert multiKeys.size() == 10;
    }

    @Test
    void testFunc08() {
        long deadline = System.currentTimeMillis() + 30 * 1000;

        insert(deadline);

        String key = KEY + 500;
        String column = COLUMN + 1;

        byte[] findValue = lsmTree.find(G.I.toBytes(key), COLUMN_FAMILY, column);
        assert Arrays.equals(findValue, G.I.toBytes(key + column));

        LockSupport.parkUntil(deadline);
        byte[] timeoutValue = lsmTree.find(G.I.toBytes(key), COLUMN_FAMILY, column);
        assert timeoutValue == null;

    }
}