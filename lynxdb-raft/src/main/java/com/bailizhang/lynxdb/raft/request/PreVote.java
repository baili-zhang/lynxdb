package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.socket.common.NioMessage;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.PRE_VOTE;
import static com.bailizhang.lynxdb.ldtp.request.RequestType.RAFT_RPC;

public class PreVote extends NioMessage {
    public PreVote(PreVoteArgs preVoteArgs) {
        super(null);

        dataBlocks.appendRawByte(RAFT_RPC);
        dataBlocks.appendRawByte(PRE_VOTE);
        dataBlocks.appendRawBuffers(preVoteArgs.toBuffers());
    }
}
