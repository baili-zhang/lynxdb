package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.socket.common.NioMessage;

import java.nio.channels.SelectionKey;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.APPEND_ENTRIES;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.RAFT_RPC;

public class AppendEntries extends NioMessage {
    public AppendEntries(SelectionKey selectionKey, AppendEntriesArgs args) {
        super(selectionKey);

        bytesList.appendRawByte(RAFT_RPC);
        bytesList.appendRawByte(APPEND_ENTRIES);
        bytesList.append(args);
    }
}
