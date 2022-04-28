package zbl.moonlight.core.raft.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.raft.request.RaftRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {
    private final static int SIZE = 20;
    private RaftLog raftLog;

    @BeforeEach
    void setUp() throws IOException {
        raftLog = new RaftLog();
        Entry[] entries = new Entry[SIZE];
        for (int i = 1; i <= SIZE; i++) {
            byte[] key = ("key" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = ("value" + i).getBytes(StandardCharsets.UTF_8);
            entries[i-1] = new Entry(i, i, RaftRequest.REQUEST_VOTE, key, value);
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
        assert entry.commitIndex() == SIZE;
        assert entry.method() == RaftRequest.REQUEST_VOTE;
        assert new String(entry.key()).equals("key" + SIZE);
        assert new String(entry.value()).equals("value" + SIZE);
    }

    @Test
    void getEntryByCommitIndex() throws IOException {
        Entry entry = raftLog.getEntryByCommitIndex(18);
        assert entry.term() == 18;
        assert entry.commitIndex() == 18;
        assert entry.method() == RaftRequest.REQUEST_VOTE;
        assert new String(entry.key()).equals("key" + 18);
        assert new String(entry.value()).equals("value" + 18);

        Entry nullEntry = raftLog.getEntryByCommitIndex(21);
        assert nullEntry == null;
    }

    @Test
    void resetLogCursor() throws IOException {
        raftLog.resetLogCursor(10);
        Entry entry = raftLog.getEntryByCommitIndex(10);
        assert entry.term() == 10;
        assert entry.commitIndex() == 10;
        assert entry.method() == RaftRequest.REQUEST_VOTE;
        assert new String(entry.key()).equals("key" + 10);
        assert new String(entry.value()).equals("value" + 10);

        Entry nullEntry = raftLog.getEntryByCommitIndex(11);
        assert nullEntry == null;


        byte[] key = ("key" + 33).getBytes(StandardCharsets.UTF_8);
        byte[] value = ("value" + 33).getBytes(StandardCharsets.UTF_8);
        Entry newEntry = new Entry(33, 33, RaftRequest.APPEND_ENTRIES, key, value);
        raftLog.append(new Entry[]{newEntry});
        Entry getNewEntry = raftLog.getEntryByCommitIndex(11);

        assert getNewEntry.term() == 33;
        assert getNewEntry.commitIndex() == 33;
        assert getNewEntry.method() == RaftRequest.APPEND_ENTRIES;
        assert new String(getNewEntry.key()).equals("key" + 33);
        assert new String(getNewEntry.value()).equals("value" + 33);
    }

    @Test
    void getEntriesByRange() throws IOException {
        Entry[] entries = raftLog.getEntriesByRange(1, 20);
        assert entries.length == 19;
        assert entries[0].term() == 2;
        assert entries[18].term() == 20;
    }
}