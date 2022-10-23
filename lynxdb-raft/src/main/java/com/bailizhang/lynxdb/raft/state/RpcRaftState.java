package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.common.RaftCommend;
import com.bailizhang.lynxdb.raft.log.LogEntry;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcRaftState extends CoreRaftState {
    private static final Logger logger = LogManager.getLogger("RpcRaftState");

    protected final AtomicInteger commitIndex = new AtomicInteger(0);

    private final HashSet<SelectionKey> votedNodes = new HashSet<>();

    private final ConcurrentHashMap<SelectionKey, Integer> nextIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SelectionKey, Integer> matchedIndex = new ConcurrentHashMap<>();

    private final Queue<RaftCommend> commendQueue = new LinkedList<>();

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
            LogEntry entry = entries.removeFirst();
            int prevLogTerm = entry.index().term();

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
            initNextIndexAndMatchIndex();
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

        int lastIndex = raftLog.maxGlobalIndex();
        if(lastLogIndex > lastIndex) {
            votedFor = candidate;
            return new RequestVoteResult(currentTerm, RequestVoteResult.IS_VOTE_GRANTED);
        } else if(lastLogIndex == lastIndex) {
            if(lastLogTerm > raftLog.lastLogTerm()) {
                votedFor = candidate;
                return new RequestVoteResult(currentTerm, RequestVoteResult.IS_VOTE_GRANTED);
            }
        }

        return new RequestVoteResult(currentTerm, RequestVoteResult.NOT_VOTE_GRANTED);
    }


    public AppendEntriesResult handleAppendEntries(int term,
                                                   ServerNode leader,
                                                   int prevLogIndex,
                                                   int prevLogTerm,
                                                   List<LogEntry> entries,
                                                   int leaderCommit) {

        if(checkTerm(term)) {
            return new AppendEntriesResult(currentTerm, AppendEntriesResult.IS_FAILED);
        }

        if(leaderNode == null) {
            leaderNode = leader;
        } else if(!leaderNode.equals(leader)) {
            return new AppendEntriesResult(currentTerm, AppendEntriesResult.IS_FAILED);
        }

        int preIndex = raftLog.maxGlobalIndex();
        int preTerm = raftLog.lastLogTerm();

        if(preIndex != prevLogIndex || preTerm != prevLogTerm) {
            return new AppendEntriesResult(currentTerm, AppendEntriesResult.IS_FAILED);
        }

        List<byte[]> commends = entries.stream().map(LogEntry::data).toList();

        stateMachine.apply0(commends);
        commitIndex.set(leaderCommit);

        return new AppendEntriesResult(currentTerm, AppendEntriesResult.IS_FAILED);
    }

    public InstallSnapshotResult handleInstallSnapshotDone() {
        return null;
    }

    public InstallSnapshotResult handleInstallSnapshotNotDone() {
        return null;
    }

    public void handleClientRequest(SelectionKey selectionKey,
                                                 int serial,
                                                 byte[] command) {
        resetElectionTimeout();

        synchronized (this) {
            int index = raftLog.append(currentTerm, command);
            RaftCommend raftCommend = new RaftCommend(selectionKey, serial, index, command);
            commendQueue.offer(raftCommend);
        }

        sendAppendEntries();
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

    private void initNextIndexAndMatchIndex() {
        nextIndex.clear();
        matchedIndex.clear();

        Set<SelectionKey> connected = raftClient.connectedNodes();
        int maxGlobalIndex = raftLog.maxGlobalIndex();

        for(SelectionKey selectionKey : connected) {
            nextIndex.put(selectionKey, maxGlobalIndex + 1);
            matchedIndex.put(selectionKey, 0);
        }
    }
}
