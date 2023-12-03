package com.bailizhang.lynxdb.socket.request;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public record SocketRequest(
        SelectionKey selectionKey,
        int serial,
        ByteBuffer[] data
) {
}
