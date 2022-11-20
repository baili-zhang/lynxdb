package com.bailizhang.lynxdb.socket.interfaces;

import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;

public interface SocketServerHandler {
    default void handleStartupCompleted() {}

    default void handleRequest(SocketRequest request) throws Exception {}
    default void handleResponse(WritableSocketResponse response) throws Exception {}

    default void handleAfterLatchAwait() {}
}
