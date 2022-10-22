package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.raft.request.RaftRequest.REQUEST_VOTE;

public record RequestVoteResult(
        int term,
        byte voteGranted
) implements BytesListConvertible {
    public static final byte IS_VOTE_GRANTED = (byte) 0x01;
    public static final byte NOT_VOTE_GRANTED = (byte) 0x02;

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(REQUEST_VOTE);
        bytesList.appendRawInt(term);
        bytesList.appendRawByte(voteGranted);

        return bytesList;
    }

    public static RequestVoteResult from(ByteBuffer buffer) {
        int term = buffer.getInt();
        byte voteGranted = buffer.get();

        return new RequestVoteResult(term, voteGranted);
    }
}
