package com.bailizhang.lynxdb.socket.interfaces;

import com.bailizhang.lynxdb.socket.response.SocketResponse;

import java.nio.channels.SelectionKey;
import java.util.concurrent.atomic.AtomicInteger;

public interface SocketClientHandler {
    default void handleConnected(SelectionKey selectionKey) throws Exception {}
    default void handleAfterLatchAwait() throws Exception {}
    default void handleBeforeSend(SelectionKey selectionKey, int serial) {}
    default void handleResponse(SocketResponse response) throws Exception {}
    default void handleConnectFailure(SelectionKey selectionKey) throws Exception {}
    default void handleDisconnect(SelectionKey selectionKey) throws Exception {}
}
