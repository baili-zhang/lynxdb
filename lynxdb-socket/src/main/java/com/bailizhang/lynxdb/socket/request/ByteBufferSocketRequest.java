package com.bailizhang.lynxdb.socket.request;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public record ByteBufferSocketRequest(
        SelectionKey selectionKey,
        int serial,
        ByteBuffer[] data
) {
}
