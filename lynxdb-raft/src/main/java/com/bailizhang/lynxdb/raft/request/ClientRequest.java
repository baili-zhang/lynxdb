package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.socket.common.NioMessage;

import java.nio.channels.SelectionKey;

import static com.bailizhang.lynxdb.raft.request.RaftRequest.CLIENT_REQUEST;

public class ClientRequest extends NioMessage {
    public ClientRequest(SelectionKey key, BytesListConvertible content) {
        super(key);
        bytesList.appendRawByte(CLIENT_REQUEST);
        bytesList.append(content);
    }
}
