package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.socket.common.NioMessage;

import static com.bailizhang.lynxdb.ldtp.result.RaftRpcResult.REQUEST_VOTE_RESULT;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.RAFT_RPC;

public class RequestVote extends NioMessage {
    public RequestVote(RequestVoteArgs args) {
        super(null);

        bytesList.appendRawByte(RAFT_RPC);
        bytesList.appendRawByte(REQUEST_VOTE_RESULT);
        bytesList.append(args);
    }
}
