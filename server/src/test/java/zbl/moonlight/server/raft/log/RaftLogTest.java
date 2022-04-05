package zbl.moonlight.server.raft.log;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import zbl.moonlight.server.mdtp.MdtpMethod;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {

    private static RaftLog raftLog;

    @BeforeAll
    static void before() throws IOException {
        raftLog = new RaftLog("data", "index");
    }

    @Test
    void testAppendByPosAndReadByPos() throws IOException {
        RaftLogEntry logEntry = new RaftLogEntry(2, 2,
                MdtpMethod.SET, "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8));
        raftLog.append(logEntry, 0);
        RaftLogEntry readLogEntry = raftLog.read(0);

        assert readLogEntry.term() == 2;
        assert readLogEntry.commitIndex() == 2;
        assert readLogEntry.method() == MdtpMethod.SET;
        assert new String(readLogEntry.key()).equals("key");
        assert new String(readLogEntry.value()).equals("value");
    }

    @Test
    void testAppendAllAndReadN() throws IOException {
        int n = 10;
        RaftLogEntry[] logEntries = new RaftLogEntry[n];

        for (int i = 0; i < n; i++) {
            logEntries[i] = new RaftLogEntry(i, i, MdtpMethod.SET,
                    ("key" + i).getBytes(StandardCharsets.UTF_8),
                    ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        raftLog.appendAll(logEntries, 0);
        RaftLogEntry[] readLogEntries = raftLog.readN(0, 10);

        for (int i = 0; i < n; i++) {
            assert readLogEntries[i].term() == i;
            assert readLogEntries[i].commitIndex() == i;
            assert readLogEntries[i].method() == MdtpMethod.SET;
            assert new String(readLogEntries[i].key()).equals("key" + i);
            assert new String(readLogEntries[i].value()).equals("value" + i);
        }
    }
}