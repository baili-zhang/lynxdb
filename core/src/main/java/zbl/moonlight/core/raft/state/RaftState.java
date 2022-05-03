package zbl.moonlight.core.raft.state;

import lombok.Setter;
import zbl.moonlight.core.raft.log.RaftLog;
import zbl.moonlight.core.raft.log.TermLog;
import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.socket.client.ServerNode;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class RaftState {
    private static final int HEARTBEAT_INTERVAL_MILLIS = 1000;
    private static final int ELECTION_INTERVAL_MILLIS = 5000;

    public RaftState(Appliable appliable, ServerNode current, List<ServerNode> nodes,
                     String logFilenamePrefix)
            throws IOException {
        stateMachine = appliable;
        currentNode = current;
        allNodes = nodes;
        otherNodes = allNodes.stream().filter((node) -> !node.equals(currentNode))
                .toList();
        raftLog = new RaftLog(logFilenamePrefix + "_index.log",
                logFilenamePrefix + "_data.log");
    }

    private volatile long heartbeatTimeMillis = System.currentTimeMillis();
    private volatile long electionTimeMillis = System.currentTimeMillis();
    public void resetHeartbeatTime() {
        heartbeatTimeMillis = System.currentTimeMillis();
    }
    public void resetElectionTime() {
        electionTimeMillis = System.currentTimeMillis();
    }
    public boolean isHeartbeatTimeout() {
        return System.currentTimeMillis() - heartbeatTimeMillis
                > HEARTBEAT_INTERVAL_MILLIS;
    }
    public boolean isElectionTimeout() {
        return System.currentTimeMillis() - electionTimeMillis
                > ELECTION_INTERVAL_MILLIS;
    }

    private final List<ServerNode> allNodes;
    /**
     * Raft 集群中的其他节点
     */
    private final List<ServerNode> otherNodes;

    /**
     * 返回 Raft 集群中的其他节点
     * @return 集群中的其他节点
     */
    public List<ServerNode> otherNodes() {
        return otherNodes;
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

    /**
     * Raft 日志
     */
    private final RaftLog raftLog;

    /**
     * 返回 raft 日志的最后一个条目
     * @return 最后一个条目
     * @throws IOException IO异常
     */
    public Entry lastEntry() throws IOException {
        return raftLog.lastEntry();
    }

    /**
     * 通过索引值获取该索引值处的日志条目
     * @param index 索引值
     * @return 日志条目
     * @throws IOException IO异常
     */
    public Entry getEntryByIndex(int index) throws IOException {
        return raftLog.getEntryByIndex(index);
    }

    /**
     * 设置最大的有效日志索引值
     * @param index 索引值
     * @throws IOException IO异常
     */
    public void setMaxIndex(int index) throws IOException {
        raftLog.setMaxIndex(index);
    }

    /**
     * 将日志条目添加到日志的尾部
     * @param entry 日志条目
     * @throws IOException IO异常
     */
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
    public void checkCommitIndex() {
        int n = (allNodes.size() >> 1) + 1;
        int lastEntryIndex = lastEntryIndex();
        for(int i = commitIndex.get() + 1; i < lastEntryIndex; i ++) {
            int count  = 0;
            for(ServerNode node : matchedIndex.keySet()) {
                if(matchedIndex.get(node) > i) {
                    count ++;
                }
            }
            if(count >= n) {
                commitIndex.set(i);
            }
        }
    }

    private final Appliable stateMachine;
    public void apply(Entry[] entries) {
        stateMachine.apply(entries);
    }
}
