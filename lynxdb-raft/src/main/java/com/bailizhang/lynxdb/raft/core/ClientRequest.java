package com.bailizhang.lynxdb.raft.core;

import com.bailizhang.lynxdb.socket.request.SocketRequest;

import java.nio.channels.SelectionKey;

public class ClientRequest extends SocketRequest {
    private final int idx;
    public ClientRequest(SelectionKey key, int idx, int serial, byte[] data) {
        super(key);

        this.idx = idx;
        this.serial = serial;
        this.data = data;
    }

    public int idx() {
        return idx;
    }

    @Override
    public void status(byte val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte status() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isKeepConnection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBroadcast() {
        throw new UnsupportedOperationException();
    }
}
