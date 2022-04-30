package zbl.moonlight.core.raft.state;

import lombok.Setter;
import zbl.moonlight.core.raft.log.RaftLog;
import zbl.moonlight.core.raft.log.TermLog;
import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.socket.client.ServerNode;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftState {
    private final List<ServerNode> allNodes;

    public RaftState(Appliable appliable, ServerNode current, List<ServerNode> nodes) throws IOException {
        stateMachine = appliable;
        currentNode = current;
        allNodes = nodes;
    }

    private final HashSet<ServerNode> votedNodes = new HashSet<>();
    public void setVotedNodeAndCheck(ServerNode serverNode) {
        synchronized (votedNodes) {
            votedNodes.add(serverNode);
            if(votedNodes.size() > (allNodes.size() >> 1)) {
                raftRole = RaftRole.Leader;
                /* 获取所有的 follower 节点 */
                List<ServerNode> followers =  allNodes.stream()
                        .filter((node) -> !node.equals(currentNode)).toList();
                nextIndex.clear();
                matchedIndex.clear();
                /* 初始化 leader 的相关属性 */
                int lastEntryIndex = lastEntryIndex();
                for (ServerNode node : followers) {
                    nextIndex.put(node, lastEntryIndex + 1);
                    matchedIndex.put(node, 0);
                }
            }
        }
    }

    private final ServerNode currentNode;
    public ServerNode currentNode() {
        return currentNode;
    }

    @Setter
    private volatile ServerNode leaderNode;
    public ServerNode leaderNode() {
        return leaderNode;
    }

    @Setter
    private volatile RaftRole raftRole = RaftRole.Follower;
    public RaftRole raftRole() {
        return raftRole;
    }

    private final RaftLog raftLog = new RaftLog();
    public Entry lastEntry() throws IOException {
        return raftLog.lastEntry();
    }
    public Entry getEntryByIndex(int index) throws IOException {
        return raftLog.getEntryByIndex(index);
    }
    public void setMaxIndex(int index) throws IOException {
        raftLog.setMaxIndex(index);
    }
    public void append(Entry entry) throws IOException {
        raftLog.append(entry);
    }
    public void append(Entry[] entries) throws IOException {
        raftLog.append(entries);
    }
    public Entry[] getEntriesByRange(int begin, int end) throws IOException {
        return raftLog.getEntriesByRange(begin, end);
    }
    public int lastEntryIndex() {
        return 0;
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
    public ConcurrentHashMap<ServerNode, Integer> nextIndex() {
        return nextIndex;
    }
    public ConcurrentHashMap<ServerNode, Integer> matchedIndex() {
        return matchedIndex;
    }

    private final Appliable stateMachine;
    public void apply(Entry[] entries) {
        stateMachine.apply(entries);
    }
}
