package zbl.moonlight.core.socket.interfaces;

import zbl.moonlight.core.socket.request.SocketRequest;

public interface SocketServerHandler {
    default void handleStartupCompleted() {}
    default void handleRequest(SocketRequest request) throws Exception {}
    default void handleAfterLatchAwait() {}
}
