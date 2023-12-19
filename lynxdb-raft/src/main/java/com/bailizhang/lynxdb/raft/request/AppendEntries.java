package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.socket.common.NioMessage;

import java.nio.channels.SelectionKey;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.APPEND_ENTRIES;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.RAFT_RPC;

public class AppendEntries extends NioMessage {
    public AppendEntries(SelectionKey selectionKey, AppendEntriesArgs args) {
        super(selectionKey);

        dataBlocks.appendRawByte(RAFT_RPC);
        dataBlocks.appendRawByte(APPEND_ENTRIES);
        dataBlocks.appendRawBuffers(args.toBuffers());
    }
}
