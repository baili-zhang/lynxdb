package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.Bytes;
import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.Flags;
import com.bailizhang.lynxdb.core.common.G;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

class LogGroupTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data/log_group_test";
    private static final String COMMAND = "hallo world";
    private static final int CAPACITY = 200;

    private LogGroup logGroup;

    @BeforeEach
    void setUp() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        LogGroupOptions options = new LogGroupOptions();
        options.regionCapacity(CAPACITY);
        logGroup = new LogGroup(BASE_DIR, options);
    }

    @AfterEach
    void tearDown() {
        logGroup.delete();
    }

    @Test
    void append() {
        for(int i = 1; i <= 450; i ++) {
            logGroup.appendEntry((COMMAND + i).getBytes(StandardCharsets.UTF_8));
        }

        int begin = 106, end = 406;
        List<LogEntry> entries = logGroup.range(begin, end);
        assert entries.size() == end - begin + 1;

        for(int i = 0; i < entries.size(); i ++) {
            LogEntry entry = entries.get(i);

            assert Arrays.equals(
                    entry.data(),
                    G.I.toBytes(COMMAND + (begin + i))
            );
        }

        for(int i = 201; i < 350; i ++) {
            logGroup.removeEntry(i);
        }

        for(int i = 201; i < 350; i ++) {
            LogEntry entry = logGroup.findEntry(i);
            assert entry.index().deleteFlag() == Flags.DELETED;
            assert Arrays.equals(entry.data(), G.I.toBytes(COMMAND + i));
        }

        logGroup.clearDeletedEntries();

        for(int i = 201; i < 350; i ++) {
            LogEntry entry = logGroup.findEntry(i);
            assert entry.index().deleteFlag() == Flags.DELETED;
            assert Arrays.equals(entry.data(), Bytes.EMPTY);
        }
    }
}