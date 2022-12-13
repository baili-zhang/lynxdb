package com.bailizhang.lynxdb.socket.interfaces;

import com.bailizhang.lynxdb.socket.request.SocketRequest;

public interface SocketServerHandler {
    default void handleStartupCompleted() {}
    default void handleRequest(SocketRequest request) throws Exception {}
    default void handleAfterLatchAwait() {}
}
