package com.bailizhang.lynxdb.socket.response;

import java.nio.channels.SelectionKey;

public class SocketResponse {
    protected final SelectionKey selectionKey;

    protected int serial;
    protected byte[] data;

    protected SocketResponse(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public SelectionKey selectionKey() {
        return selectionKey;
    }

    public int serial() {
        return serial;
    }

    public byte[] data() {
        return data;
    }
}
