package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.socket.common.NioMessage;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.PRE_VOTE;
import static com.bailizhang.lynxdb.ldtp.request.RequestType.RAFT_RPC;

public class PreVote extends NioMessage {
    public PreVote(PreVoteArgs preVoteArgs) {
        super(null);

        bytesList.appendRawByte(RAFT_RPC);
        bytesList.appendRawByte(PRE_VOTE);
        bytesList.append(preVoteArgs);
    }
}
