package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.socket.common.NioMessage;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.REQUEST_VOTE;

public class RequestVote extends NioMessage {
    public RequestVote(RequestVoteArgs args) {
        super(null);

        bytesList.appendRawByte(REQUEST_VOTE);
        bytesList.append(args);
    }
}
