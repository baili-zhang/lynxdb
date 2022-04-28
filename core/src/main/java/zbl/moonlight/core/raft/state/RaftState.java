package zbl.moonlight.core.raft.state;

import lombok.Setter;
import zbl.moonlight.core.raft.log.RaftLog;
import zbl.moonlight.core.raft.log.TermLog;
import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.socket.client.ServerNode;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftState {
    public RaftState(Appliable appliable) throws IOException {
        stateMachine = appliable;
    }

    @Setter
    private volatile RaftRole raftRole = RaftRole.Follower;

    private final RaftLog raftLog = new RaftLog();
    public Entry lastEntry() {
        return raftLog.lastEntry();
    }
    public Entry getEntryByCommitIndex(int commitIndex) {
        return raftLog.getEntryByCommitIndex(commitIndex);
    }
    public void resetLogCursor(int index) {
        raftLog.resetLogCursor(index);
    }
    public void append(Entry[] entries) {
        raftLog.append(entries);
    }
    public Entry[] getEntriesByRange(int begin, int end) {
        return raftLog.getEntriesByRange(begin, end);
    }
    private final TermLog termLog = new TermLog();
    public int currentTerm() {
        return termLog.currentTerm();
    }
    public void setCurrentTerm(int term) {
        termLog.setCurrentTerm(term);
    }
    public ServerNode voteFor() {
        return termLog.voteFor();
    }

    private final AtomicInteger commitIndex = new AtomicInteger(0);
    private final AtomicInteger lastApplied = new AtomicInteger(0);

    public int commitIndex() {
        return commitIndex.get();
    }
    public void setCommitIndex(int index) {
        commitIndex.set(index);
    }
    public int lastApplied() {
        return lastApplied.get();
    }

    private final ConcurrentHashMap<ServerNode, Integer> nextIndex
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ServerNode, Integer> matchedIndex
            = new ConcurrentHashMap<>();

    private final Appliable stateMachine;
    public void apply(Entry[] entries) {
        stateMachine.apply(entries);
    }
}
