package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.socket.common.NioMessage;

import java.nio.channels.SelectionKey;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.JOIN_CLUSTER;
import static com.bailizhang.lynxdb.ldtp.request.RequestType.RAFT_RPC;

public class JoinCluster extends NioMessage {
    public JoinCluster(SelectionKey key, JoinClusterArgs args) {
        super(key);

        dataBlocks.appendRawByte(RAFT_RPC);
        dataBlocks.appendRawByte(JOIN_CLUSTER);
        dataBlocks.appendRawBuffers(args.toBuffers());
    }
}
