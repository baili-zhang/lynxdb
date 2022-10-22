package com.bailizhang.lynxdb.socket.client;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.socket.code.Request;
import com.bailizhang.lynxdb.socket.common.NioMessage;

import java.nio.channels.SelectionKey;

public class ClientRequest extends NioMessage {
    public ClientRequest(SelectionKey key, BytesListConvertible content) {
        super(key);
        bytesList.appendRawByte(Request.CLIENT_REQUEST);
        bytesList.append(content);
    }

    public ClientRequest(SelectionKey key, BytesList list) {
        super(key);
        bytesList.appendRawByte(Request.CLIENT_REQUEST);
        bytesList.append(list);
    }
}
