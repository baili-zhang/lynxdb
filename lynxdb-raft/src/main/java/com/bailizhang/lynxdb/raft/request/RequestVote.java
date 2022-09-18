package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.socket.common.NioMessage;

import java.nio.channels.SelectionKey;

import static com.bailizhang.lynxdb.raft.request.RaftRequest.REQUEST_VOTE;

public class RequestVote extends NioMessage {
    public RequestVote(RequestVoteArgs args) {
        super(null);

        bytesList.appendRawByte(REQUEST_VOTE);
        bytesList.append(args);
    }
}
