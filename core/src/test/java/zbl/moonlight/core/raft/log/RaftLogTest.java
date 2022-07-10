package zbl.moonlight.core.raft.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zbl.moonlight.core.raft.request.Entry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

class RaftLogTest {
    private final static int SIZE = 20;
    private RaftLog raftLog;

    @BeforeEach
    void setUp() throws IOException {
        raftLog = new RaftLog();
        Entry[] entries = new Entry[SIZE];
        for (int i = 1; i <= SIZE; i++) {
            byte[] command = ("command" + i).getBytes(StandardCharsets.UTF_8);
            entries[i-1] = new Entry(i, command);
        }
        raftLog.append(entries);
    }

    @AfterEach
    void tearDown() throws IOException {
        raftLog.delete();
    }

    @Test
    void lastEntry() throws IOException {
        Entry entry = raftLog.lastEntry();
        assert entry.term() == SIZE;
        assert new String(entry.command()).equals("command" + SIZE);
    }

    @Test
    void getEntryByIndex() throws IOException {
        Entry entry = raftLog.getEntryByIndex(18);
        assert entry.term() == 18;
        assert new String(entry.command()).equals("command" + 18);

        Entry nullEntry = raftLog.getEntryByIndex(21);
        assert nullEntry == null;
    }

    @Test
    void resetLogCursor() throws IOException {
        raftLog.setMaxIndex(10);
        Entry entry = raftLog.getEntryByIndex(10);
        assert entry.term() == 10;
        assert new String(entry.command()).equals("command" + 10);

        Entry nullEntry = raftLog.getEntryByIndex(11);
        assert nullEntry == null;


        byte[] command = ("command" + 33).getBytes(StandardCharsets.UTF_8);
        Entry newEntry = new Entry(33, command);
        raftLog.append(new Entry[]{newEntry});
        Entry getNewEntry = raftLog.getEntryByIndex(11);

        assert getNewEntry.term() == 33;
        assert new String(getNewEntry.command()).equals("command" + 33);
    }

    @Test
    void getEntriesByRange() throws IOException {
        Entry[] entries = raftLog.getEntriesByRange(1, 20);
        assert entries.length == 19;
        assert entries[0].term() == 2;
        assert entries[18].term() == 20;
    }
}