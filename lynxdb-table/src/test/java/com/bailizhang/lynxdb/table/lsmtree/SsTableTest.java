package com.bailizhang.lynxdb.table.lsmtree;

import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogGroupOptions;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.table.lsmtree.sstable.SsTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

class SsTableTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data/sstable_test";
    private static final String VALUE_LOG_DIR = "/values";

    private SsTable ssTable;

    @BeforeEach
    void setUp() {
        LogGroupOptions logGroupOptions = new LogGroupOptions();
        logGroupOptions.regionCapacity(200);

        LogGroup valueLogGroup = new LogGroup(VALUE_LOG_DIR, logGroupOptions);
        ssTable = new SsTable(Path.of(BASE_DIR), 9, valueLogGroup);
    }

    @AfterEach
    void tearDown() {
        FileUtils.delete(Path.of(BASE_DIR));
    }

    @Test
    void testFunc01() {

    }
}