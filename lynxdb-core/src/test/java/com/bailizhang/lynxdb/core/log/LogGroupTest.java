package com.bailizhang.lynxdb.core.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

class LogGroupTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/logs";

    private static final int TERM = 20;
    private static final String COMMAND = "hallo world";

    private LogGroup logGroup;

    @BeforeEach
    void setUp() {
        LogGroupOptions options = new LogGroupOptions();
        logGroup = new LogGroup(BASE_DIR, options);
    }

    @AfterEach
    void tearDown() {
        logGroup.delete();
    }

    @Test
    void append() {
        for(int i = 1; i <= 450; i ++) {
            logGroup.append((COMMAND + i).getBytes(StandardCharsets.UTF_8));
        }

        int begin = 106, end = 406;
        List<LogEntry> entries = logGroup.range(begin, end);
        assert entries.size() == end - begin + 1;

        for(int i = 0; i < entries.size(); i ++) {
            LogEntry entry = entries.get(i);

            assert Arrays.equals(
                    entry.data(),
                    (COMMAND + (begin + i)).getBytes(StandardCharsets.UTF_8)
            );
        }
    }
}