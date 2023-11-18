package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.socket.common.NioMessage;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.REQUEST_VOTE;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.RAFT_RPC;

public class RequestVote extends NioMessage {
    public RequestVote(RequestVoteArgs args) {
        super(null);

        dataBlocks.appendRawByte(RAFT_RPC);
        dataBlocks.appendRawByte(REQUEST_VOTE);
        dataBlocks.appendRawBuffers(args.toBuffers());
    }
}
