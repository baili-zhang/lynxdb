package com.bailizhang.lynxdb.core.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

class LogRegionTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/logs";

    private static final int TERM = 20;
    private static final byte[] COMMAND = "hallo world".getBytes(StandardCharsets.UTF_8);

    private LogRegion logRegion;

    @BeforeEach
    void setUp() {
        try {
            logRegion = LogRegion.create(1, 30, BASE_DIR);
        } catch (Exception ignore) {
            logRegion = LogRegion.open(1, BASE_DIR);
        }
    }

    @AfterEach
    void tearDown() {
        logRegion.delete();
    }

    @Test
    void begin() {
        assert logRegion.globalIndexBegin() == 30;
    }

    @Test
    void end() {
        assert logRegion.globalIndexEnd() == 29;
    }

    @Test
    void append() {
        logRegion.append(TERM, COMMAND);
        assert logRegion.globalIndexBegin() == 30;
        assert logRegion.globalIndexEnd() == 30;

        LogEntry entry = logRegion.readEntry(30);
        assert entry.index().term() == TERM;
        assert Arrays.equals(entry.data(), COMMAND);
    }
}