package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.log.LogEntry;
import com.bailizhang.lynxdb.raft.log.LogRegion;
import com.bailizhang.lynxdb.raft.request.AppendEntries;
import com.bailizhang.lynxdb.raft.request.AppendEntriesArgs;
import com.bailizhang.lynxdb.raft.request.RequestVote;
import com.bailizhang.lynxdb.raft.request.RequestVoteArgs;
import com.bailizhang.lynxdb.raft.result.AppendEntriesResult;
import com.bailizhang.lynxdb.raft.result.InstallSnapshotResult;
import com.bailizhang.lynxdb.raft.result.RequestVoteResult;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcRaftState extends CoreRaftState {
    private static final Logger logger = LogManager.getLogger("RpcRaftState");

    protected final AtomicInteger commitIndex = new AtomicInteger(0);

    private final HashSet<SelectionKey> votedNodes = new HashSet<>();

    private final ConcurrentHashMap<SelectionKey, Integer> nextIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SelectionKey, Integer> matchedIndex = new ConcurrentHashMap<>();

    private volatile ServerNode votedFor = null;

    private RaftClient raftClient;
    private RaftServer raftServer;

    public void sendRequestVote() {
        RequestVoteArgs args = new RequestVoteArgs(
                currentTerm,
                currentNode,
                lastLogIndex(),
                lastLogTerm()
        );

        RequestVote requestVote = new RequestVote(args);
        raftClient.broadcast(requestVote);
    }

    public void sendAppendEntries() {
        Set<SelectionKey> connected = raftClient.connectedNodes();

        for(SelectionKey selectionKey : connected) {
            int next = nextIndex.get(selectionKey);
            int matched = matchedIndex.get(selectionKey);

            LinkedList<LogEntry> entries = raftLog.range(matched, next - 1);
            int prevLogTerm = entries.removeFirst().term();

            AppendEntriesArgs args = new AppendEntriesArgs(
                    currentTerm,
                    currentNode,
                    matched,
                    prevLogTerm,
                    entries,
                    commitIndex.get()
            );

            AppendEntries appendEntries = new AppendEntries(selectionKey, args);
            raftClient.send(appendEntries);
        }
    }

    public void voteGranted(int term, SelectionKey selectionKey) {
        if(checkTerm(term)) {
            return;
        }

        votedNodes.add(selectionKey);
        List<ServerNode> serverNodes = stateMachine.clusterNodes();
        if(votedNodes.size() >= (serverNodes.size() >> 1) + 1) {
            transformToLeader();
            startTimeout();
        }
    }

    public void voteNotGranted(int term, SelectionKey selectionKey) {
        SocketAddress address;

        try {
            address = ((SocketChannel)selectionKey.channel()).getRemoteAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        logger.info("Vote not granted, term: {}, server node: {}", term, address);
    }

    public void appendEntriesSuccess(SelectionKey selectionKey) {
        int newMatchIndex = nextIndex.get(selectionKey);
        matchedIndex.put(selectionKey, newMatchIndex);
    }

    public void appendEntriesFailed(SelectionKey selectionKey) {
        int oldMatchIndex = matchedIndex.get(selectionKey);
        matchedIndex.put(selectionKey, oldMatchIndex - 1);
    }

    public RequestVoteResult handleRequestVote(int term, ServerNode candidate,
                                               int lastLogIndex, int lastLogTerm) {
        if(checkTerm(term)) {
            return new RequestVoteResult(currentTerm, RequestVoteResult.NOT_VOTE_GRANTED);
        }

        if(currentTerm == term && votedFor != null) {
            return new RequestVoteResult(currentTerm, RequestVoteResult.NOT_VOTE_GRANTED);
        }

        if(electionTimeoutTimes.get() == 0) {
            return new RequestVoteResult(currentTerm, RequestVoteResult.NOT_VOTE_GRANTED);
        }

        LogRegion region = raftLog.lastEntry();
        int lastIndex = region.end();
        if(lastLogIndex > lastIndex) {
            votedFor = candidate;
            return new RequestVoteResult(currentTerm, RequestVoteResult.IS_VOTE_GRANTED);
        } else if(lastLogIndex == lastIndex) {
            LogEntry entry = region.readIndex(lastIndex);
            if(lastLogTerm > entry.term()) {
                votedFor = candidate;
                return new RequestVoteResult(currentTerm, RequestVoteResult.IS_VOTE_GRANTED);
            }
        }

        return new RequestVoteResult(currentTerm, RequestVoteResult.NOT_VOTE_GRANTED);
    }


    public AppendEntriesResult handleAppendEntries() {
        return null;
    }

    public InstallSnapshotResult handleInstallSnapshotDone() {
        return null;
    }

    public InstallSnapshotResult handleInstallSnapshotNotDone() {
        return null;
    }

    public void handleClientRequest(byte[] command) {
    }

    public int commitIndex() {
        return commitIndex.get();
    }

    public void commitIndex(int index) {
        commitIndex.set(index);
    }

    public void raftClient(RaftClient client) {
        if(raftClient == null) {
            raftClient = client;
        }
    }

    public void raftServer(RaftServer server) {
        if(raftServer == null) {
            raftServer = server;
            stateMachine.raftServer(server);
        }
    }
}
