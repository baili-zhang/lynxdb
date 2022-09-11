package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.result.AppendEntriesResult;
import com.bailizhang.lynxdb.raft.result.InstallSnapshotResult;
import com.bailizhang.lynxdb.raft.result.RequestVoteResult;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcRaftState extends CoreRaftState {
    protected final AtomicInteger commitIndex = new AtomicInteger(0);
    private final HashSet<SelectionKey> votedNodes = new HashSet<>();

    private final ConcurrentHashMap<SelectionKey, Integer> nextIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SelectionKey, Integer> matchedIndex = new ConcurrentHashMap<>();

    private RaftClient raftClient;
    private RaftServer raftServer;

    public void sendRequestVote() {

    }

    public void sendAppendEntries() {

    }

    public void voteGranted(SelectionKey selectionKey) {

    }

    public void voteNotGranted(SelectionKey selectionKey) {

    }

    public void appendEntriesSuccess(SelectionKey selectionKey) {
    }

    public void appendEntriesFailed(SelectionKey selectionKey) {
    }

    public RequestVoteResult handleRequestVote(int term, ServerNode candidate,
                                               int lastLogIndex, int lastLogTerm) {

        return null;
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
