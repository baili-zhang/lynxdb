package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.socket.common.NioMessage;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.PRE_VOTE;

public class PreVote extends NioMessage {
    public PreVote(PreVoteArgs preVoteArgs) {
        super(null);

        bytesList.appendRawByte(PRE_VOTE);
        bytesList.append(preVoteArgs);
    }
}
