package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.socket.client.ServerNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CoreRaftState extends TimeoutRaftState {
    private static final Logger logger = LogManager.getLogger("CoreRaftState");

    protected final ServerNode currentNode;

    protected volatile ServerNode leaderNode;

    protected CoreRaftState() {
        currentNode = raftConfiguration.currentNode();
    }

    public boolean checkTerm(int term) {
        if(term > currentTerm) {
            currentTerm(term);
            transformToFollower();
            return false;
        }
        return true;
    }

    public ServerNode currentNode() {
        return currentNode;
    }

    public ServerNode leaderNode() {
        return leaderNode;
    }

    public boolean hasLeader() {
        return leaderNode != null;
    }

    public void leaderNode(ServerNode node) {
        leaderNode = node;
    }
}
