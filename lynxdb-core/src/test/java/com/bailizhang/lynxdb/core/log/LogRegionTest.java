package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

class LogRegionTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/logs";

    private static final int GLOBAL_INDEX_BEGIN = 30;
    private static final int LOG_ENTRY_COUNT = 100;
    private static final String COMMAND = "command";

    private LogRegion logRegion;

    @BeforeEach
    void setUp() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        LogGroupOptions options = new LogGroupOptions(INT_LENGTH);

        FileUtils.createDirIfNotExisted(BASE_DIR);

        logRegion = new LogRegion(1, BASE_DIR, options);
        logRegion.globalIndexBegin(GLOBAL_INDEX_BEGIN);
        logRegion.globalIndexEnd(GLOBAL_INDEX_BEGIN - 1);
    }

    @AfterEach
    void tearDown() {
        FileUtils.delete(Path.of(BASE_DIR));
    }

    @Test
    void append() {
        for(int i = 0; i < LOG_ENTRY_COUNT; i ++) {
            byte[] extraData = BufferUtils.toBytes(i);
            logRegion.append(extraData, G.I.toBytes(COMMAND + i));
        }

        for(int i = 0; i < LOG_ENTRY_COUNT; i ++) {
            LogEntry entry = logRegion.readEntry(GLOBAL_INDEX_BEGIN + i);
            assert Arrays.equals(entry.index().extraData(), BufferUtils.toBytes(i));
            assert Arrays.equals(entry.data(), G.I.toBytes(COMMAND + i));
        }
    }
}