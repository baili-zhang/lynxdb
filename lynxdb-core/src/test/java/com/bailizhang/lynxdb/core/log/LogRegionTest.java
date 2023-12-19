package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

class LogRegionTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data/log_region_test";

    private static final int LOG_ENTRY_COUNT = 200;
    private static final int REPEAT_TIMES = 1;
    private static final String COMMAND = "command";

    private LogRegion logRegion;

    @BeforeEach
    void setUp() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        LogGroupOptions options = new LogGroupOptions();
        options.regionCapacity(LOG_ENTRY_COUNT);
        options.regionBlockSize(1000);

        FileUtils.createDirIfNotExisted(BASE_DIR);

        logRegion = new LogRegion(1, BASE_DIR, options);
    }

    @AfterEach
    void tearDown() {
        FileUtils.delete(Path.of(BASE_DIR));
    }

    @Test
    void append() {
        for(int i = 0; i < LOG_ENTRY_COUNT; i ++) {
            String temp = COMMAND.repeat(REPEAT_TIMES) + i;
            logRegion.appendEntry(G.I.toBytes(temp));
        }

        int globalIndexBegin = logRegion.globalIdxBegin();

        for(int i = globalIndexBegin; i < LOG_ENTRY_COUNT + globalIndexBegin; i ++) {
            LogEntry entry = logRegion.readEntry(i);
            String temp = COMMAND.repeat(REPEAT_TIMES) + (i - globalIndexBegin);
            assert Arrays.equals(entry.data(), G.I.toBytes(temp));
        }
    }
}