package com.bailizhang.lynxdb.core.utils;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public interface SocketUtils {
    static SocketAddress address(SelectionKey selectionKey) {
        SocketChannel channel = (SocketChannel)selectionKey.channel();
        try {
            return channel.getRemoteAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
