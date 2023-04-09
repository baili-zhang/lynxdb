package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.socket.common.NioMessage;

import java.nio.channels.SelectionKey;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.INSTALL_SNAPSHOT;

public class InstallSnapshot extends NioMessage {
    public InstallSnapshot(SelectionKey selectionKey, InstallSnapshotArgs args) {
        super(selectionKey);

        bytesList.appendRawByte(INSTALL_SNAPSHOT);
        bytesList.append(args);
    }
}
