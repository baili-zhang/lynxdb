/*
 * Copyright 2023 Baili Zhang.
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

package com.bailizhang.lynxdb.table.lsmtree;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.Flags;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogGroupOptions;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.table.config.LsmTreeOptions;
import com.bailizhang.lynxdb.table.exception.DeletedException;
import com.bailizhang.lynxdb.table.exception.TimeoutException;
import com.bailizhang.lynxdb.table.lsmtree.sstable.KeyEntry;
import com.bailizhang.lynxdb.table.lsmtree.sstable.SsTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class SsTableTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data/sstable_test";
    private static final String VALUE_LOG_DIR = System.getProperty("user.dir") + "/data/sstable_test/values";

    private LogGroup valueLogGroup;
    private SsTable ssTable;

    @BeforeEach
    void setUp() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));

        LogGroupOptions logGroupOptions = new LogGroupOptions();
        logGroupOptions.regionCapacity(200);

        valueLogGroup = new LogGroup(VALUE_LOG_DIR, logGroupOptions);
    }

    @AfterEach
    void tearDown() {
        Path dirPath = Path.of(BASE_DIR);
        if(FileUtils.notExist(dirPath)) {
            return;
        }
        FileUtils.delete(dirPath);
    }

    void create() {
        Path dirPath = Path.of(BASE_DIR);
        LsmTreeOptions options = new LsmTreeOptions(BASE_DIR, 200);

        List<KeyEntry> keyEntries = new ArrayList<>();

        for(int i = 0; i < 2000; i ++) {
            byte[] key = G.I.toBytes("key" + i);
            byte[] value = G.I.toBytes("value" + i);

            int valueGlobalIdx = valueLogGroup.appendEntry(value);

            KeyEntry keyEntry = new KeyEntry(
                    Flags.EXISTED,
                    key,
                    value,
                    valueGlobalIdx,
                    -1
            );
            keyEntries.add(keyEntry);
        }

        keyEntries.sort(KeyEntry::compareTo);

        ssTable = SsTable.create(
                dirPath,
                3,
                2,
                options,
                keyEntries,
                valueLogGroup
        );
    }

    @Test
    void testFunc01() {
        Assertions.assertThrowsExactly(
                RuntimeException.class,
                () -> new SsTable(Path.of(BASE_DIR), 9, valueLogGroup)
        );
    }

    @Test
    void testFunc02() throws DeletedException, TimeoutException {
        create();

        byte[] key = G.I.toBytes("key" + 1999);
        byte[] value = ssTable.find(key);

        assert Arrays.equals(value, G.I.toBytes("value" + 1999));

        key = G.I.toBytes("key" + 19999);
        value = ssTable.find(key);

        assert value == null;
    }

    @Test
    void testFunc03() {
        create();

        for(int i = 0; i < 2000; i ++) {
            byte[] key = G.I.toBytes("key" + i);
            assert ssTable.bloomFilterContains(key);
        }

        byte[] key = G.I.toBytes("key" + 19999);
        assert !ssTable.bloomFilterContains(key);
    }
}