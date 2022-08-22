package zbl.moonlight.socket.interfaces;

import zbl.moonlight.socket.request.SocketRequest;

public interface SocketServerHandler {
    default void handleStartupCompleted() {}
    default void handleRequest(SocketRequest request) throws Exception {}
    default void handleAfterLatchAwait() {}
}
