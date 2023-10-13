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
    private static final String BASE_DIR = System.getProperty("user.dir") + "/logs";

    private static final int LOG_ENTRY_COUNT = 100;
    private static final String COMMAND = "command";

    private LogRegion logRegion;
    private LogGroupOptions options;

    @BeforeEach
    void setUp() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        options = new LogGroupOptions();
        options.regionCapacity(200);

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
            String temp = COMMAND.repeat(1024) + i;
            logRegion.appendEntry(G.I.toBytes(temp));
        }

        int globalIndexBegin = options.regionCapacityOrDefault(0) * logRegion.id();

        for(int i = globalIndexBegin; i < LOG_ENTRY_COUNT + globalIndexBegin; i ++) {
            LogEntry entry = logRegion.readEntry(i);
            String temp = COMMAND.repeat(1024) + (i - globalIndexBegin);
            assert Arrays.equals(entry.data(), G.I.toBytes(temp));
        }
    }
}