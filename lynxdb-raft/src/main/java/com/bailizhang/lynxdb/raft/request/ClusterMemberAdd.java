package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.socket.common.NioMessage;

import java.nio.channels.SelectionKey;

import static com.bailizhang.lynxdb.raft.request.RaftRequest.CLUSTER_MEMBER_ADD;

public class ClusterMemberAdd extends NioMessage {
    public ClusterMemberAdd(SelectionKey key, ClusterMemberAddArgs args) {
        super(key);

        bytesList.appendRawByte(CLUSTER_MEMBER_ADD);
        bytesList.append(args);
    }
}
