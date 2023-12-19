package com.bailizhang.lynxdb.socket.interfaces;

import com.bailizhang.lynxdb.socket.request.SegmentSocketRequest;

public interface SocketServerHandler {
    default void handleStartupCompleted() {}
    default void handleRequest(SegmentSocketRequest request) throws Exception {}
    default void handleAfterLatchAwait() {}
}
