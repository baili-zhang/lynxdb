package zbl.moonlight.server.raft.log;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import zbl.moonlight.server.mdtp.MdtpMethod;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {

    private static RaftLog raftLog;

    @BeforeAll
    static void append() throws IOException {
        raftLog = new RaftLog("data", "index", "verify");

        int n = 20;
        RaftLogEntry[] logEntries = new RaftLogEntry[n];

        for (int i = 0; i < n; i++) {
            logEntries[i] = new RaftLogEntry(i, i, MdtpMethod.SET,
                    ("key" + i).getBytes(StandardCharsets.UTF_8),
                    ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        raftLog.appendAll(logEntries, 0);
    }

    @AfterAll
    static void close() throws IOException {
        raftLog.close();
    }

    @Test
    void testRead() throws IOException {
        int i = 0;
        RaftLogEntry readLogEntry = raftLog.read(i);

        assert readLogEntry.term() == i;
        assert readLogEntry.commitIndex() == i;
        assert readLogEntry.method() == MdtpMethod.SET;
        assert new String(readLogEntry.key()).equals("key" + i);
        assert new String(readLogEntry.value()).equals("value" + i);
    }

    @Test
    void testReadN() throws IOException {
        int n = 10;
        RaftLogEntry[] readLogEntries = raftLog.readN(0, n);

        for (int i = 0; i < n; i++) {
            assert readLogEntries[i].term() == i;
            assert readLogEntries[i].commitIndex() == i;
            assert readLogEntries[i].method() == MdtpMethod.SET;
            assert new String(readLogEntries[i].key()).equals("key" + i);
            assert new String(readLogEntries[i].value()).equals("value" + i);
        }
    }

    @Test
    void testAppendAndRead() throws IOException {
        int index = 20;
        RaftLogEntry entry = new RaftLogEntry(index, index, MdtpMethod.SET,
                ("key" + index).getBytes(StandardCharsets.UTF_8),
                ("value" + index).getBytes(StandardCharsets.UTF_8));
        raftLog.append(entry);

        RaftLogEntry readLogEntry = raftLog.read(index);
        assert readLogEntry.term() == index;
        assert readLogEntry.commitIndex() == index;
        assert readLogEntry.method() == MdtpMethod.SET;
        assert new String(readLogEntry.key()).equals("key" + index);
        assert new String(readLogEntry.value()).equals("value" + index);
    }

    @Test
    void testAppendAndReadByIndex() throws IOException {
        int index = 21, cursor = 11;
        RaftLogEntry entry = new RaftLogEntry(index, index, MdtpMethod.SET,
                ("key" + index).getBytes(StandardCharsets.UTF_8),
                ("value" + index).getBytes(StandardCharsets.UTF_8));
        raftLog.append(entry, cursor);

        RaftLogEntry readLogEntry = raftLog.read(cursor);
        assert readLogEntry.term() == index;
        assert readLogEntry.commitIndex() == index;
        assert readLogEntry.method() == MdtpMethod.SET;
        assert new String(readLogEntry.key()).equals("key" + index);
        assert new String(readLogEntry.value()).equals("value" + index);
    }
}